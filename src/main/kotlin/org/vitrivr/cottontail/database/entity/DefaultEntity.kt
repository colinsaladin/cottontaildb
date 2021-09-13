package org.vitrivr.cottontail.database.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.forEach

import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.database.catalogue.storeName
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.general.*
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.basics.IndexState
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.math.min

/**
 * Represents a single entity in the Cottontail DB data model. An [DefaultEntity] has name that must remain unique within a [DefaultSchema].
 * The [DefaultEntity] contains one to many [Column]s holding the actual data. Hence, it can be seen as a table containing tuples.
 *
 * Calling the default constructor for [DefaultEntity] opens that [DefaultEntity]. It can only be opened once due to file locks and it
 * will remain open until the [Entity.close()] method is called.
 *
 * @see DefaultSchema
 * @see Column
 * @see EntityTx
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultEntity(override val name: Name.EntityName, override val parent: DefaultSchema) : Entity {

    /** A [DefaultEntity] belongs to the same [DefaultCatalogue] as the [DefaultSchema] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [DefaultEntity]. */
    override val version: DBOVersion
        get() = DBOVersion.V3_0

    /** Number of [Column]s in this [DefaultEntity]. */
    override val numberOfColumns: Int
        get() = this.catalogue.environment.computeInTransaction { tx ->
            DefaultCatalogue.readEntryForEntity(this.name, this.catalogue, tx).columns.size
        }

    /** Estimated maximum [TupleId]s for this [DefaultEntity]. This is a snapshot and may change immediately after calling this method. */
    override val maxTupleId: TupleId
        get() = this.catalogue.environment.computeInTransaction { tx ->
            DefaultCatalogue.readSequence(Name.EntityName.objectToEntry(this.name), this.catalogue, tx)
                ?: throw DatabaseException.DataCorruptionException("Sequence entry for entity ${this@DefaultEntity.name} is missing.")
        }

    /** Number of entries in this [DefaultEntity]. This is a snapshot and may change immediately. */
    override val numberOfRows: Long
        get() = this.catalogue.environment.computeInTransaction { tx ->
            val entityStore = this.catalogue.environment.openStore(this.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, tx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open entity store for entity ${this.name}.")
            entityStore.count(tx)
        }

    /** Status indicating whether this [DefaultEntity] is open or closed. */
    override val closed: Boolean
        get() = this.parent.closed

    /**
     * Creates and returns a new [DefaultEntity.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultEntity.Tx] for.
     * @return New [DefaultEntity.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * A [Tx] that affects this [DefaultEntity]. Opening a [DefaultEntity.Tx] will automatically spawn [ColumnTx]
     * and [IndexTx] for every [Column] and [IndexTx] associated with this [DefaultEntity].
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), EntityTx {

        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DefaultEntity
            get() = this@DefaultEntity

        /** Internal reference to [Store] required for accessing entity metadata. */
        private val entityStore: Store = this@DefaultEntity.parent.parent.environment.openStore(this@DefaultEntity.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx)

        /** Map of [Name.ColumnName] to [Column]. */
        private val columns: Map<Name.ColumnName, Column<*>>

        /** Checks if DBO is still open. */
        init {
            val entityEntry = try {
                DefaultCatalogue.readEntryForEntity(this@DefaultEntity.name, this@DefaultEntity.catalogue, this.context.xodusTx)
            } catch (e: DatabaseException.EntityDoesNotExistException) {
                throw DatabaseException.DataCorruptionException("Catalogue entry for entity ${this@DefaultEntity.name} is missing.")
            }

            /* Load a (ordered) map of columns. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            this.columns = entityEntry.columns.associateWith {
                try {
                    val columnEntry = DefaultCatalogue.readEntryForColumn(it, this@DefaultEntity.catalogue, this.context.xodusTx)
                    DefaultColumn(columnEntry.toColumnDef(), this@DefaultEntity)
                } catch (e: DatabaseException.ColumnDoesNotExistException) {
                    throw DatabaseException.DataCorruptionException("Catalogue entry for column $it is missing.")
                }
            }
        }

        /**
         * Reads the values of one or many [Column]s and returns it as a [Record]
         *
         * @param tupleId The [TupleId] of the desired entry.
         * @param columns The [ColumnDef]s that should be read.
         * @return The desired [Record].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record = this.withReadLock {
            /* Read values from underlying columns. */
            val values = columns.map {
                val column = this.columns[it.name] ?: throw IllegalArgumentException("Column ${it.name} does not exist on entity ${this@DefaultEntity.name}.")
                (this.context.getTx(column) as ColumnTx<*>).get(tupleId)
            }.toTypedArray()

            /* Return value of all the desired columns. */
            return StandaloneRecord(tupleId, columns, values)
        }

        /**
         * Returns the number of entries in this [DefaultEntity].
         *
         * @return The number of entries in this [DefaultEntity].
         */
        override fun count(): Long = this.entityStore.count(this.context.xodusTx)

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun maxTupleId(): TupleId = DefaultCatalogue.readSequence(Name.EntityName.objectToEntry(this@DefaultEntity.name), this@DefaultEntity.catalogue, this.context.xodusTx)
            ?: throw DatabaseException.DataCorruptionException("Sequence entry for entity ${this@DefaultEntity.name} is missing.")

        /**
         * Lists all [Column]s for the [DefaultEntity] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        override fun listColumns(): List<ColumnDef<*>> =  this.columns.values.map { it.columnDef }.toList()

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        override fun columnForName(name: Name.ColumnName): Column<*> {
            return this.columns[name] ?: throw DatabaseException.ColumnDoesNotExistException(name)
        }

        /**
         * Lists all [Name.IndexName] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun listIndexes(): List<Index>  {
            val store = this@DefaultEntity.parent.parent.environment.openStore(DefaultCatalogue.CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to load index catalogue.")
            val list = mutableListOf<Index>()
            val cursor = store.openCursor(this.context.xodusTx)
            cursor.getSearchKeyRange(Name.EntityName.objectToEntry(this@DefaultEntity.name))
            cursor.forEach {
                val entry = IndexCatalogueEntry.entryToObject(this.value) as IndexCatalogueEntry
                list.add(entry.type.open(entry.name, this@DefaultEntity))
            }
            return list
        }

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun indexForName(name: Name.IndexName): Index {
            val entry = DefaultCatalogue.readEntryForIndex(name, this@DefaultEntity.catalogue, this.context.xodusTx)
            return entry.type.open(entry.name, this@DefaultEntity)
        }

        /**
         * Creates the [Index] with the given settings
         *
         * @param name [Name.IndexName] of the [Index] to create.
         * @param type Type of the [Index] to create.
         * @param columns The list of [columns] to [Index].
         */
        override fun createIndex(name: Name.IndexName, type: IndexType, columns: Array<Name.ColumnName>, params: Map<String, String>): Index {
            try {
                DefaultCatalogue.readEntryForIndex(name, this@DefaultEntity.catalogue, this.context.xodusTx)
                throw DatabaseException.IndexAlreadyExistsException(name)
            } catch (e: DatabaseException.IndexDoesNotExistException) {
                /* Ignore. */
            }

            /* Create index catalogue entry. */
            val indexEntry = IndexCatalogueEntry(name, type, IndexState.DIRTY, columns, params)
            if (!DefaultCatalogue.writeEntryForIndex(indexEntry, this@DefaultEntity.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("CREATE index $name failed: Failed to create catalogue entry.")
            }

            /* Create index store entry. */
            if (this@DefaultEntity.catalogue.environment.openStore(name.storeName(), indexEntry.type.storeConfig(), this.context.xodusTx, true) == null) {
                throw DatabaseException.DataCorruptionException("CREATE index $name failed: Failed to create store.")
            }
            return type.open(name, this@DefaultEntity)
        }

        /**
         * Drops the [Index] with the given name.
         *
         * @param name [Name.IndexName] of the [Index] to drop.
         */
        override fun dropIndex(name: Name.IndexName) = this.withWriteLock {
            val indexKey = Name.IndexName.objectToEntry(name)

            /* Open index catalogue. */
            val environment = this.dbo.parent.parent.environment
            val indexCatalogue = environment.openStore(DefaultCatalogue.CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to load index catalogue.")

            /* Read entity catalogue entry (does it exist?) */
            val indexEntry = IndexCatalogueEntry.entryToObject(indexCatalogue.get(this.context.xodusTx, indexKey) ?: throw DatabaseException.IndexDoesNotExistException(name)) as IndexCatalogueEntry
            if (!indexCatalogue.delete(this.context.xodusTx, indexKey))
                throw DatabaseException.DataCorruptionException("DROP index $name failed: Failed to delete catalogue entry.")

            /* Remove store. */
            environment.removeStore(name.storeName(), this.context.xodusTx)
        }

        /**
         * Optimizes the [DefaultEntity] underlying this [Tx]. Optimization involves rebuilding of [Index]es and statistics.
         */
        override fun optimize() {
            TODO()
        }

        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns
         * all [TupleId]s contained within the surrounding [DefaultEntity].
         *
         * <strong>Important:</strong> It remains to the caller to close the [Iterator]
         *
         * @param columns The [ColumnDef]s that should be scanned.
         *
         * @return [Iterator]
         */
        override fun scan(columns: Array<ColumnDef<*>>): Iterator<Record> = scan(columns, 0, 1)

        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns all [TupleId]s
         * contained within the surrounding [DefaultEntity] and a certain range.
         *
         * @param columns The [ColumnDef]s that should be scanned.
         * @param partitionIndex The [partitionIndex] for this [scan] call.
         * @param partitions The total number of partitions for this [scan] call.
         *
         * @return [Iterator]
         */
        override fun scan(columns: Array<ColumnDef<*>>, partitionIndex: Int, partitions: Int) = object : Iterator<Record> {

            /** The maximum [TupleId] to iterate over. */
            private var now: TupleId

            /** The maximum [TupleId] to iterate over. */
            private val end: TupleId

            /** List of [ColumnTx]s used by  this [Iterator]. */
            private val txs = columns.map {
                val column = this@Tx.columns[it.name] ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@DefaultEntity.name}.")
                (this@Tx.context.getTx(column) as ColumnTx<*>)
            }

            /** The wrapped [Iterator] of the first column. */
            private val cursor: Cursor = this@Tx.entityStore.openCursor(this@Tx.context.xodusTx)

            /** Array of [Value]s emitted by this [Iterator]. */
            private val values = arrayOfNulls<Value?>(columns.size)

            init {
                val maximum: Long = this@Tx.maxTupleId()
                val partitionSize: Long = Math.floorDiv(maximum, partitions.toLong()) + 1L
                this.cursor.getSearchKeyRange(LongBinding.longToCompressedEntry(partitionIndex * partitionSize)) /* Set start of cursor. */
                this.now = LongBinding.compressedEntryToLong(this.cursor.key)
                this.end = min(((partitionIndex + 1) * partitionSize), maximum)
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                for ((i, tx) in this.txs.withIndex()) {
                    this.values[i] = tx.get(this.now)
                }
                return StandaloneRecord(this.now, columns, this.values)
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                return if (this.cursor.getNext()) {
                    this.now = LongBinding.compressedEntryToLong(this.cursor.key)
                    return this.now < this.end
                } else {
                    false
                }
            }
        }

        /**
         * Insert the provided [Record].
         *
         * @param record The [Record] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         *
         * @throws TxException If some of the [Tx] on [Column] or [Index] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        override fun insert(record: Record): TupleId {
            /* Execute INSERT on entity level. */
            val nextTupleId = DefaultCatalogue.nextSequence(Name.EntityName.objectToEntry(this@DefaultEntity.name), this@DefaultEntity.catalogue, this.context.xodusTx)
                ?: throw DatabaseException.DataCorruptionException("Sequence entry for entity ${this@DefaultEntity.name} is missing.")
            this.entityStore.putRight(this.context.xodusTx, LongBinding.longToCompressedEntry(nextTupleId), ByteBinding.BINDING.objectToEntry(0.toByte()))

            /* Execute INSERT on column level. */
            val inserts = Object2ObjectArrayMap<Name.ColumnName, Value>(this.columns.size)
            this.columns.values.forEach {
                val tx = this.context.getTx(it) as ColumnTx<Value>
                val value = record[it.columnDef]
                inserts[it.columnDef.name] = value

                /* Check if null value is allowed. */
                if (value == null && !it.nullable) throw DatabaseException("Cannot insert NULL value into column ${it.columnDef}.")
                if (value != null) tx.put(nextTupleId, value)
            }

            /* Issue DataChangeEvent.InsertDataChange event and update indexes + statistics. */
            val event = Operation.DataManagementOperation.InsertOperation(this.context.txId, this@DefaultEntity.name, nextTupleId, inserts)
            //this.snapshot.indexes.values.forEach { (this.context.getTx(it) as IndexTx).update(event) }
            //this.snapshot.statistics.consume(event)
            this.context.signalEvent(event)

            return nextTupleId
        }

        /**
         * Updates the provided [Record] (identified based on its [TupleId]). Columns specified in the [Record] that are not part
         * of the [DefaultEntity] will cause an error! This will set this [DefaultEntity.Tx] to [TxStatus.DIRTY].
         *
         * @param record The [Record] that should be updated
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun update(record: Record) {
            /* Check existence of Record on entity level. */
            if (!this.entityStore.exists(this.context.xodusTx, LongBinding.longToCompressedEntry(record.tupleId), ByteBinding.BINDING.objectToEntry(0.toByte()))) {
                throw DatabaseException("Record with tuple ID ${record.tupleId} cannot be updated because it doesn't exist on entity ${this@DefaultEntity.name}.")
            }

            /* Execute UPDATE on column level. */
            val updates = Object2ObjectArrayMap<Name.ColumnName, Pair<Value?, Value?>>(record.columns.size)
            record.columns.forEach { def ->
                val column = this.columns[def.name] ?: throw DatabaseException("Record with tuple ID ${record.tupleId} cannot be updated because column $def does not exist on entity ${this@DefaultEntity.name}.")
                val value = record[def]
                val columnTx = (this.context.getTx(column) as ColumnTx<Value>)
                if (value == null) {
                    if (!def.nullable) throw DatabaseException("Record with tuple ID ${record.tupleId} cannot be updated with NULL value for column $def, because column is not nullable.")
                    updates[def.name] = Pair(columnTx.delete(record.tupleId), value) /* Map: ColumnDef -> Pair[Old, New]. */
                } else {
                    updates[def.name] = Pair(columnTx.put(record.tupleId, value), value) /* Map: ColumnDef -> Pair[Old, New]. */
                }
            }

            /* Issue DataChangeEvent.UpdateDataChangeEvent and update indexes + statistics. */
            val event = Operation.DataManagementOperation.UpdateOperation(this.context.txId, this@DefaultEntity.name, record.tupleId, updates)
            //this.snapshot.indexes.values.forEach { (this.context.getTx(it) as IndexTx).update(event) }
            //this.snapshot.statistics.consume(event)
            this.context.signalEvent(event)
        }

        /**
         * Deletes the entry with the provided [TupleId]. This will set this [DefaultEntity.Tx] to [TxStatus.DIRTY]
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun delete(tupleId: TupleId) = this.withWriteLock {
            /* Perform DELETE on entity level. */
            if (!this.entityStore.delete(this.context.xodusTx, LongBinding.longToCompressedEntry(tupleId))) {
                throw DatabaseException("Record with tuple ID $tupleId cannot be deleted because it doesn't exist on entity ${this@DefaultEntity.name}.")
            }

            /* Perform DELETE on column level. */
            val deleted = Object2ObjectArrayMap<Name.ColumnName, Value?>(this.columns.size)
            this.columns.values.map {
                deleted[it.name] = (this.context.getTx(it) as ColumnTx<*>).delete(tupleId)
            }

            /* Issue DataChangeEvent.DeleteDataChangeEvent and update indexes + statistics. */
            val event = Operation.DataManagementOperation.DeleteOperation(this.context.txId, this@DefaultEntity.name, tupleId, deleted)
            //this.snapshot.indexes.values.forEach { (this.context.getTx(it) as IndexTx).update(event) }
            //this.snapshot.statistics.consume(event)
            this.context.signalEvent(event)
        }
    }
}
