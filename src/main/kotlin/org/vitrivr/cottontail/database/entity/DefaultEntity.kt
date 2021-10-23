package org.vitrivr.cottontail.database.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.catalogue.entries.*
import org.vitrivr.cottontail.database.catalogue.storeName
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.column.DefaultColumn
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.Cursor
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.IndexState
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.database.operations.Operation
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.concurrent.withLock
import kotlin.math.min

/**
 * The default [Entity] implementation based on JetBrains Xodus.
 *
 * @see Entity
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
            EntityCatalogueEntry.read(this.name, this.catalogue, tx)?.columns?.size
                ?: throw DatabaseException.DataCorruptionException("Catalogue entry for entity ${this@DefaultEntity.name} is missing.")
        }

    /** Estimated maximum [TupleId]s for this [DefaultEntity]. This is a snapshot and may change immediately after calling this method. */
    override val maxTupleId: TupleId
        get() = this.catalogue.environment.computeInTransaction { tx ->
            SequenceCatalogueEntries.read(this.sequenceName, this.catalogue, tx)
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

    /** The [Name.SequenceName] for this [DefaultEntity]*/
    private val sequenceName: Name.SequenceName = this@DefaultEntity.name.tid()

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

        /** Map of [Name.ColumnName] to [Column]. */
        private val columns: Map<Name.ColumnName,ColumnTx<*>>

        /** Map of [Name.IndexName] to [IndexTx]. */
        private val indexes: Map<Name.IndexName,IndexTx>

        /**
         * Obtains a global (non-exclusive) read-lock on [DefaultCatalogue].
         *
         * Prevents [DefaultCatalogue] from being closed while transaction is ongoing.
         */
        private val closeStamp = this.dbo.catalogue.closeLock.readLock()

        init {
            /** Checks if DBO is still open. */
            if (this.dbo.closed) {
                this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this.dbo)
            }

            /* Load entity entry.  */
            val entityEntry = EntityCatalogueEntry.read(this@DefaultEntity.name, this@DefaultEntity.catalogue, this.context.xodusTx)
                ?: throw DatabaseException.DataCorruptionException("Catalogue entry for entity ${this@DefaultEntity.name} is missing.")

            /* Load a (ordered) map of columns. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            this.columns = entityEntry.columns.associateWith {
                val columnEntry = ColumnCatalogueEntry.read(it, this@DefaultEntity.catalogue, this.context.xodusTx)
                    ?: throw DatabaseException.DataCorruptionException("Catalogue entry for column $it is missing.")
                this.context.getTx(DefaultColumn(columnEntry.toColumnDef(), this@DefaultEntity)) as ColumnTx<*>
            }

            /* Load a map of indexes. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            this.indexes = entityEntry.indexes.associateWith {
                val indexEntry = IndexCatalogueEntry.read(it, this@DefaultEntity.catalogue, this.context.xodusTx)
                    ?: throw DatabaseException.DataCorruptionException("Catalogue entry for index $it is missing.")
                this.context.getTx(indexEntry.type.open(it, this.dbo)) as IndexTx
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
        override fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record = this.txLatch.withLock {
            /* Read values from underlying columns. */
            val values = columns.map {
                val tx = this.columns[it.name] ?: throw IllegalArgumentException("Column ${it.name} does not exist on entity ${this@DefaultEntity.name}.")
                tx.get(tupleId)
            }.toTypedArray()

            /* Return value of all the desired columns. */
            return StandaloneRecord(tupleId, columns, values)
        }

        /**
         * Returns the number of entries in this [DefaultEntity].
         *
         * @return The number of entries in this [DefaultEntity].
         */
        override fun count(): Long = this.txLatch.withLock {
            this.columns.values.first().count()
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun maxTupleId(): TupleId = this.txLatch.withLock {
            SequenceCatalogueEntries.read(this@DefaultEntity.sequenceName, this@DefaultEntity.catalogue, this.context.xodusTx)
                ?: throw DatabaseException.DataCorruptionException("Sequence entry for entity ${this@DefaultEntity.name} is missing.")
        }

        /**
         * Lists all [Column]s for the [DefaultEntity] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        override fun listColumns(): List<ColumnDef<*>> = this.txLatch.withLock {
            this.columns.values.map { it.columnDef }
        }

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        override fun columnForName(name: Name.ColumnName): Column<*> = this.txLatch.withLock {
            return this.columns[name]?.dbo ?: throw DatabaseException.ColumnDoesNotExistException(name)
        }

        /**
         * Lists all [Name.IndexName] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun listIndexes(): List<Name.IndexName> = this.txLatch.withLock {
            this.indexes.keys.toList()
        }

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun indexForName(name: Name.IndexName): Index = this.txLatch.withLock {
            this.indexes[name]?.dbo ?: throw DatabaseException.IndexDoesNotExistException(name)
        }

        /**
         * Creates the [Index] with the given settings
         *
         * @param name [Name.IndexName] of the [Index] to create.
         * @param type Type of the [Index] to create.
         * @param columns The list of [columns] to [Index].
         */
        override fun createIndex(name: Name.IndexName, type: IndexType, columns: List<Name.ColumnName>, params: Map<String, String>): Index = this.txLatch.withLock {
            /* Checks if index exists. */
            if (IndexCatalogueEntry.exists(name, this@DefaultEntity.catalogue, this.context.xodusTx)) {
                throw DatabaseException.IndexAlreadyExistsException(name)
            }

            /* Create index catalogue entry. */
            val indexEntry = IndexCatalogueEntry(name, type, IndexState.DIRTY, columns, params)
            if (!IndexCatalogueEntry.write(indexEntry, this@DefaultEntity.catalogue, this.context.xodusTx)) {
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
        override fun dropIndex(name: Name.IndexName) = this.txLatch.withLock {
            /* Check if index exists. */
            if (!IndexCatalogueEntry.exists(name, this@DefaultEntity.catalogue, this.context.xodusTx)) {
                throw DatabaseException.IndexDoesNotExistException(name)
            }

            /* Open index catalogue. */
            if (!IndexCatalogueEntry.delete(name, this@DefaultEntity.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("DROP index $name failed: Failed to delete catalogue entry.")
            }

            /* Remove index store. */
            this@DefaultEntity.catalogue.environment.removeStore(name.storeName(), this.context.xodusTx)
        }

        /**
         * Optimizes the [DefaultEntity] underlying this [Tx]. Optimization involves rebuilding of [Index]es and statistics.
         */
        override fun optimize() = this.txLatch.withLock {
            val statistics = this.columns.values.map { /* Reset column statistics. */
                val stat = it.statistics() as ValueStatistics<Value>
                stat.reset()
                stat
            }

            /* Iterate over all entries and update indexes and statistics. */
            val cursor = this.cursor(this.columns.values.map { it.columnDef }.toTypedArray())
            while (cursor.moveNext()) {
                var i = 0
                cursor.value().forEach { _, v -> statistics[i++].insert(v) }
            }

            /* Write new statistics values. */
            this.columns.values.forEachIndexed { i, c ->
                val entry = StatisticsCatalogueEntry.read(c.columnDef.name, this@DefaultEntity.catalogue, this.context.xodusTx)
                    ?: throw DatabaseException.DataCorruptionException("Failed to DELETE value from ${c.columnDef.name}: Reading column statistics failed.")
                StatisticsCatalogueEntry.write(entry = entry.copy(statistics = statistics[i]), catalogue = this@DefaultEntity.catalogue, transaction = this.context.xodusTx)
            }

            /* Close all the opened cursors. */
            cursor.close()
        }

        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns
         * all [TupleId]s contained within the surrounding [DefaultEntity].
         *
         * <strong>Important:</strong> It remains to the caller to close the [Iterator]
         *
         * @param columns The [ColumnDef]s that should be scanned.
         *
         * @return [Cursor]
         */
        override fun cursor(columns: Array<ColumnDef<*>>): Cursor<Record> = cursor(columns, 0, 1)

        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns all [TupleId]s
         * contained within the surrounding [DefaultEntity] and a certain range.
         *
         * @param columns The [ColumnDef]s that should be scanned.
         * @param partitionIndex The [partitionIndex] for this [cursor] call.
         * @param partitions The total number of partitions for this [cursor] call.
         *
         * @return [Cursor]
         */
        override fun cursor(columns: Array<ColumnDef<*>>, partitionIndex: Int, partitions: Int) = object : Cursor<Record> {

            /** Array of [Value]s emitted by this [Iterator]. */
            private val values = arrayOfNulls<Value?>(columns.size)

            /** The current [TupleId] this [Cursor] is pointin to. */
            private var current: TupleId

            /** The maximum [TupleId] to iterate over. */
            private val end: TupleId

            /** The wrapped [Cursor] to iterate over columns. */
            private val cursors: List<Cursor<out Value?>>

            /** Flag indicating that this [Cursor] has been moved recently but the underlying data has not been fetched yet. */
            private var dataInvalid: Boolean = true

            init {
                val maximum: Long = this@Tx.maxTupleId()
                val partitionSize: Long = Math.floorDiv(maximum, partitions.toLong()) + 1L
                this.end = min(((partitionIndex + 1) * partitionSize), maximum)
                this.cursors = columns.map {
                    this@Tx.columns[it.name]?.cursor(partitionIndex * partitionSize, this.end) ?: throw IllegalStateException("Column $it missing in transaction.")
                }
                this.current = this.cursors.first().key()
            }

            /**
             * Returns the [TupleId] this [Cursor] is currently pointing to.
             */
            override fun key(): TupleId = this.current

            /**
             * Returns the [Record] this [Cursor] is currently pointing to.
             */
            override fun value(): Record {
                if (this.dataInvalid) {
                    this.cursors.forEachIndexed { index, cursor -> this.values[index] = cursor.value() }
                    this.dataInvalid = false
                }
                return StandaloneRecord(this.current, columns, this.values)
            }

            /**
             * Tries to move this [Cursor]. Returns true on success and false otherwise.
             */
            override fun moveNext(): Boolean {
                if (this.current >= this.end) return false
                this.dataInvalid = true
                return this.cursors.all { it.moveNext() }
            }

            /**
             * Closes this [Cursor].
             */
            override fun close() {
                this.dataInvalid = true
                this.cursors.forEach { it.close() }
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
        override fun insert(record: Record): TupleId = this.txLatch.withLock {
            /* Execute INSERT on entity level. */
            val nextTupleId = SequenceCatalogueEntries.next(this@DefaultEntity.sequenceName, this@DefaultEntity.catalogue, this.context.xodusTx)
                ?: throw DatabaseException.DataCorruptionException("Sequence entry for entity ${this@DefaultEntity.name} is missing.")

            /* Execute INSERT on column level. */
            val inserts = Object2ObjectArrayMap<ColumnDef<*>, Value>(this.columns.size)
            this.columns.values.forEach { tx ->
                val value = record[tx.columnDef]
                inserts[tx.columnDef] = value

                /* Check if null value is allowed. */
                if (value == null && !tx.columnDef.nullable) throw DatabaseException("Cannot insert NULL value into column ${tx.columnDef}.")
                (tx as ColumnTx<Value>).add(nextTupleId, value)
            }

            /* Issue DataChangeEvent.InsertDataChange event and update indexes + statistics. */
            val event = Operation.DataManagementOperation.InsertOperation(this.context.txId, this@DefaultEntity.name, nextTupleId, inserts)
            this.indexes.values.forEach { it.update(event) }
            this.context.signalEvent(event)

            return nextTupleId
        }

        /**
         * Updates the provided [Record] (identified based on its [TupleId]). Columns specified in the [Record] that are not part
         * of the [DefaultEntity] will cause an error!
         *
         * @param record The [Record] that should be updated
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun update(record: Record) = this.txLatch.withLock {
            /* Execute UPDATE on column level. */
            val updates = Object2ObjectArrayMap<ColumnDef<*>, Pair<Value?, Value?>>(record.columns.size)
            record.columns.forEach { def ->
                val columnTx = this.columns[def.name] ?: throw DatabaseException("Record with tuple ID ${record.tupleId} cannot be updated because column $def does not exist on entity ${this@DefaultEntity.name}.")
                val value = record[def]
                if (value == null && !def.nullable) throw DatabaseException("Record with tuple ID ${record.tupleId} cannot be updated with NULL value for column $def, because column is not nullable.")
                updates[def] = Pair((columnTx as ColumnTx<Value>).update(record.tupleId, value), value) /* Map: ColumnDef -> Pair[Old, New]. */
            }

            /* Issue DataChangeEvent.UpdateDataChangeEvent and update indexes + statistics. */
            val event = Operation.DataManagementOperation.UpdateOperation(this.context.txId, this@DefaultEntity.name, record.tupleId, updates)
            this.indexes.values.forEach { it.update(event) }
            this.context.signalEvent(event)
        }

        /**
         * Deletes the entry with the provided [TupleId].
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun delete(tupleId: TupleId) = this.txLatch.withLock {
            /* Perform DELETE on column level. */
            val deleted = Object2ObjectArrayMap<ColumnDef<*>, Value?>(this.columns.size)
            this.columns.values.map {
                deleted[it.columnDef] = it.delete(tupleId)
            }

            /* Issue DataChangeEvent.DeleteDataChangeEvent and update indexes + statistics. */
            val event = Operation.DataManagementOperation.DeleteOperation(this.context.txId, this@DefaultEntity.name, tupleId, deleted)
            this.indexes.values.forEach { it.update(event) }
            this.context.signalEvent(event)
        }

        /**
         * Called when a transaction finalizes. Releases the lock held on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}
