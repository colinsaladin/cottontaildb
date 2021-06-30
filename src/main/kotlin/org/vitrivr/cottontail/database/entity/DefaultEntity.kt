package org.vitrivr.cottontail.database.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.DB
import org.mapdb.DBException
import org.mapdb.StoreWAL
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.general.*
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.database.statistics.entity.EntityStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.*
import java.util.concurrent.locks.StampedLock
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
 * @version 2.1.0
 */
class DefaultEntity(override val path: Path, override val parent: Schema) : Entity {
    /**
     * Companion object of the [DefaultEntity]
     */
    companion object {
        /** Filename for the [DefaultEntity] catalogue.  */
        const val CATALOGUE_FILE = "index.db"

        /** Field name for the [DefaultEntity] header field.  */
        const val ENTITY_HEADER_FIELD = "cdb_entity_header"

        /** Field name for the [DefaultEntity]'s statistics.  */
        const val ENTITY_STATISTICS_FIELD = "cdb_entity_statistics"

        /**
         * Initializes an empty [DefaultEntity] on disk.
         *
         * @param name The [Name.EntityName] of the [DefaultEntity]
         * @param path The [Path] under which to create the [DefaultEntity]
         * @param config The [Config] for to use for creation.
         * @param columns The list of [ColumnDef] to [ColumnEngine] pairs.
         */
        fun initialize(name: Name.EntityName, path: Path, config: Config, columns: List<Pair<ColumnDef<*>, ColumnEngine>>): Path {
            /* Prepare empty catalogue. */
            val dataPath = TxFileUtilities.createPath(path.resolve("entity_${name.simple}"))
            if (Files.exists(dataPath)) throw DatabaseException.InvalidFileException("Failed to create entity '$name'. Data directory '$dataPath' seems to be occupied.")
            Files.createDirectories(dataPath)

            /* Prepare column references and column statistics. */
            try {
                val entityHeader = EntityHeader(name = name.simple, columns = columns.map {
                    val colPath = dataPath.resolve("${it.first.name.simple}.col")
                    it.second.create(colPath, it.first, config)
                    EntityHeader.ColumnRef(it.first.name.simple, it.second)
                })
                val entityStatistics = EntityStatistics()
                columns.forEach { entityStatistics[it.first] = it.first.type.statistics() as ValueStatistics<Value> }

                /* Write entity header + statistics entry to disk. */
                config.mapdb.db(dataPath.resolve(CATALOGUE_FILE)).use {
                    it.atomicVar(ENTITY_HEADER_FIELD, EntityHeader.Serializer).create().set(entityHeader)
                    it.atomicVar(ENTITY_STATISTICS_FIELD, EntityStatistics.Serializer).create().set(entityStatistics)
                    it.commit()
                }
            } catch (e: DBException) {
                TxFileUtilities.delete(dataPath) /* Cleanup. */
                throw DatabaseException("Failed to create entity '$name' due to error in the underlying data store: {${e.message}")
            } catch (e: IOException) {
                TxFileUtilities.delete(dataPath) /* Cleanup. */
                throw DatabaseException("Failed to create entity '$name' due to an IO exception: {${e.message}")
            } catch (e: Throwable) {
                TxFileUtilities.delete(dataPath) /* Cleanup. */
                throw DatabaseException("Failed to create entity '$name' due to an unexpected error: {${e.message}")
            }

            return dataPath
        }
    }

    /** Internal reference to the [StoreWAL] underpinning this [DefaultEntity]. */
    private val store: DB = try {
        this.parent.parent.config.mapdb.db(this.path.resolve(CATALOGUE_FILE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open entity at $path: ${e.message}'.")
    }

    /** The [EntityHeader] of this [DefaultEntity]. */
    private val headerField = this.store.atomicVar(ENTITY_HEADER_FIELD, EntityHeader.Serializer).createOrOpen()

    /** The maximum [TupleId] in this [DefaultEntity]. */
    private val statisticsField = this.store.atomicVar(ENTITY_STATISTICS_FIELD, EntityStatistics.Serializer).createOrOpen()

    /** The [Name.EntityName] of this [DefaultEntity]. */
    override val name: Name.EntityName = this.parent.name.entity(this.headerField.get().name)

    /** An internal lock that is used to synchronize access to this [DefaultEntity] in presence of ongoing [Tx]. */
    private val closeLock = StampedLock()

    /** List of all the [Column]s associated with this [DefaultEntity]; Iteration order of entries as defined in schema! */
    private val columns: MutableMap<Name.ColumnName, Column<*>> = Object2ObjectLinkedOpenHashMap()

    /** List of all the [Index]es associated with this [DefaultEntity]. */
    private val indexes: MutableMap<Name.IndexName, Index> = Object2ObjectOpenHashMap()

    /** The [DBOVersion] of this [DefaultEntity]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** The [EntityStatistics] in this [DefaultEntity]. This is a snapshot and may change immediately. */
    override val statistics: EntityStatistics
        get() = this.statisticsField.get()

    /** Number of [Column]s in this [DefaultEntity]. */
    override val numberOfColumns: Int
        get() = this.columns.size

    /** Number of entries in this [DefaultEntity]. This is a snapshot and may change immediately. */
    override val numberOfRows: Long
        get() = this.statistics.count

    /** Estimated maximum [TupleId]s for this [DefaultEntity]. This is a snapshot and may change immediately. */
    override val maxTupleId: TupleId
        get() = this.statistics.maximumTupleId

    /** Status indicating whether this [DefaultEntity] is open or closed. */
    override val closed: Boolean
        get() = this.store.isClosed()

    init {
        /** Load and initialize the columns. */
        val header = this.headerField.get()
        header.columns.map {
            val columnName = this.name.column(it.name)
            val path = this.path.resolve("${it.name}.col")
            this.columns[columnName] = it.type.open(path, this)
        }

        /** Load and initialize the indexes. */
        header.indexes.forEach {
            val indexName = this.name.index(it.name)
            val path = this.path.resolve("${it.name}.idx")
            indexes[indexName] = it.type.open(path, this)
        }
    }

    /**
     * Creates and returns a new [DefaultEntity.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultEntity.Tx] for.
     * @return New [DefaultEntity.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * Closes the [DefaultEntity].
     *
     * Closing an [DefaultEntity] is a delicate matter since ongoing [DefaultEntity.Tx] objects as well
     * as all involved [Column]s are involved.Therefore, access to the method is mediated by an
     * global [DefaultEntity] wide lock.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.store.close()
            this.columns.values.forEach { it.close() }
            this.indexes.values.forEach { it.close() }
        }
    }

    /**
     * A [Tx] that affects this [DefaultEntity]. Opening a [DefaultEntity.Tx] will automatically spawn [ColumnTx]
     * and [IndexTx] for every [Column] and [IndexTx] associated with this [DefaultEntity].
     *
     * @author Ralph Gasser
     * @version 1.3.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), EntityTx {

        /** Obtains a global non-exclusive lock on [DefaultEntity]. Prevents [DefaultEntity] from being closed. */
        private val closeStamp = this@DefaultEntity.closeLock.readLock()

        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DefaultEntity
            get() = this@DefaultEntity

        /**
         * [TxSnapshot] of this [EntityTx]
         *
         * Important: The [EntityTxSnapshot] is created lazily upon first access, which means that whatever
         * caller creates it, it holds the necessary locks!
         */
        override val snapshot by lazy {
            object : EntityTxSnapshot {
                /** Local snapshot of the surrounding [Entity]'s [EntityStatistics]. */
                override val statistics: EntityStatistics = this@DefaultEntity.statisticsField.get()

                /** Local snapshot of the surrounding [Entity]'s [Index]es. */
                override val indexes = Object2ObjectOpenHashMap(this@DefaultEntity.indexes)

                /** A map of all [TxAction] executed by this [EntityTx]. Can be seen as an in-memory WAL. */
                override val actions = LinkedList<TxAction>()

                /**
                 * Commits the [EntityTx] and integrates all changes made through it into the [DefaultEntity].
                 */
                override fun commit() {
                    try {
                        /* Materialize changes in surrounding schema (in-memory). */
                        this.actions.forEach { it.commit() }

                        /* Update update header and commit changes. */
                        val oldHeader = this@DefaultEntity.headerField.get()
                        val newHeader = oldHeader.copy(modified = System.currentTimeMillis(), indexes = this.indexes.values.map { EntityHeader.IndexRef(it.name.simple, it.type) })

                        /* Write header + statistics and commit. */
                        this@DefaultEntity.headerField.compareAndSet(oldHeader, newHeader)
                        this@DefaultEntity.statisticsField.set(this.statistics)
                        this@DefaultEntity.store.commit()
                    } catch (e: Throwable) {
                        this@Tx.status = TxStatus.ERROR
                        throw DatabaseException("Failed to create index '$name' due to a storage exception: ${e.message}")
                    }
                }

                /**
                 * Rolls back the [EntityTx] and reverts all changes.
                 */
                override fun rollback() {
                    this.actions.forEach { it.rollback() }
                    this@DefaultEntity.store.rollback()
                }

                /**
                 * Records a [TxAction] with this [EntityTxSnapshot].
                 *
                 * @param action The [TxAction] to record.
                 * @return True on success, false otherwise.
                 */
                override fun record(action: TxAction): Boolean = when (action) {
                    is CreateIndexTxAction,
                    is DropIndexTxAction -> {
                        this.actions.add(action)
                        true
                    }
                    else -> false
                }
            }
        }

        /** Checks if DBO is still open. */
        init {
            if (this@DefaultEntity.closed) {
                this@DefaultEntity.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this@DefaultEntity)
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
                val column = this@DefaultEntity.columns[it.name] ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@DefaultEntity.name}.")
                (this.context.getTx(column) as ColumnTx<*>).read(tupleId)
            }.toTypedArray()

            /* Return value of all the desired columns. */
            return StandaloneRecord(tupleId, columns, values)
        }

        /**
         * Returns the number of entries in this [DefaultEntity].
         *
         * @return The number of entries in this [DefaultEntity].
         */
        override fun count(): Long = this.withReadLock {
            return this.snapshot.statistics.count
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun maxTupleId(): TupleId = this.withReadLock {
            return this.snapshot.statistics.maximumTupleId
        }

        /**
         * Lists all [Column]s for the [DefaultEntity] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        override fun listColumns(): List<Column<*>> = this.withReadLock {
            return this@DefaultEntity.columns.values.toList()
        }

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        override fun columnForName(name: Name.ColumnName): Column<*> = this.withReadLock {
            if (!name.wildcard) {
                this@DefaultEntity.columns[name] ?: throw DatabaseException.ColumnDoesNotExistException(name)
            } else {
                val fqn = this@DefaultEntity.name.column(name.simple)
                this@DefaultEntity.columns[fqn] ?: throw DatabaseException.ColumnDoesNotExistException(fqn)
            }
        }

        /**
         * Lists all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun listIndexes(): List<Index> = this.withReadLock {
            this.snapshot.indexes.values.toList()
        }

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun indexForName(name: Name.IndexName): Index = this.withReadLock {
            this.snapshot.indexes[name] ?: throw DatabaseException.IndexDoesNotExistException(name)
        }

        /**
         * Creates the [Index] with the given settings
         *
         * @param name [Name.IndexName] of the [Index] to create.
         * @param type Type of the [Index] to create.
         * @param columns The list of [columns] to [Index].
         */
        override fun createIndex(name: Name.IndexName, type: IndexType, columns: Array<ColumnDef<*>>, params: Map<String, String>): Index = this.withWriteLock {
            if (this.snapshot.indexes.containsKey(name)) {
                throw DatabaseException.IndexAlreadyExistsException(name)
            }

            /* Creates and opens the index and adds it to snapshot. */
            try {
                val data = this@DefaultEntity.path.resolve("${name.simple}.idx")
                val index = type.create(data, this.dbo, name, columns, params)

                /* Update snapshot. */
                this.snapshot.indexes[name] = index
                this.snapshot.record(CreateIndexTxAction(index))

                return index
            } catch (e: DatabaseException) {
                this.status = TxStatus.ERROR
                throw e
            }
        }

        /**
         * Drops the [Index] with the given name.
         *
         * @param name [Name.IndexName] of the [Index] to drop.
         */
        override fun dropIndex(name: Name.IndexName) = this.withWriteLock {
            /* Obtain index and acquire exclusive lock on it. */
            val index = this.snapshot.indexes.remove(name) ?: throw DatabaseException.IndexDoesNotExistException(name)
            this.context.requestLock(index, LockMode.EXCLUSIVE)

            /* Remove entity from local snapshot. */
            this.snapshot.indexes.remove(name)
            this.snapshot.record(DropIndexTxAction(name))
            Unit
        }

        /**
         * Optimizes the [DefaultEntity] underlying this [Tx]. Optimization involves rebuilding of [Index]es and statistics.
         */
        override fun optimize() = this.withWriteLock {
            /* Stage 1a: Rebuild incremental indexes and statistics. */
            val incremental = this.snapshot.indexes.values.filter {
                it.supportsIncrementalUpdate
            }.map {
                val tx = this.context.getTx(it) as IndexTx
                tx.clear() /* Important: Clear indexes. */
                tx
            }
            val columns = this@DefaultEntity.columns.values.map { it.columnDef }.toTypedArray()
            val map = Object2ObjectOpenHashMap<ColumnDef<*>, Value>(columns.size)
            val iterator = this.scan(columns)
            this.snapshot.statistics.reset()
            iterator.forEach { r ->
                r.forEach { columnDef, value -> map[columnDef] = value }
                val event = DataChangeEvent.InsertDataChangeEvent(this@DefaultEntity, r.tupleId, map) /* Fake data change event for update. */
                this.snapshot.statistics.consume(event)
                incremental.forEach { it.update(event) }
            }

            /* Stage 2: Rebuild remaining indexes. */
            this.snapshot.indexes.values.filter {
                !it.supportsIncrementalUpdate
            }.forEach {
                (this.context.getTx(it) as IndexTx).rebuild()
            }
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
        override fun scan(columns: Array<ColumnDef<*>>, partitionIndex: Int, partitions: Int) = this@Tx.withReadLock {
            object : Iterator<Record> {

                /** The [LongRange] to iterate over. */
                private val range: LongRange

                init {
                    val maximum: Long = this@Tx.maxTupleId()
                    val partitionSize: Long = Math.floorDiv(maximum, partitions.toLong()) + 1L
                    val start: Long = partitionIndex * partitionSize
                    val end = min(((partitionIndex + 1) * partitionSize), maximum)
                    this.range = start..end
                }

                /** List of [ColumnTx]s used by  this [Iterator]. */
                private val txs = columns.map {
                    val column = this@DefaultEntity.columns[it.name] ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@DefaultEntity.name}.")
                    (this@Tx.context.getTx(column) as ColumnTx<*>)
                }

                /** The wrapped [Iterator] of the first column. */
                private val wrapped = this.txs.first().scan(this.range)

                /** Array of [Value]s emitted by this [DefaultEntity]. */
                private val values = arrayOfNulls<Value?>(columns.size)

                /**
                 * Returns the next element in the iteration.
                 */
                override fun next(): Record {
                    val tupleId = this.wrapped.next()
                    for ((i, tx) in this.txs.withIndex()) {
                        this.values[i] = tx.read(tupleId)
                    }
                    return StandaloneRecord(tupleId, columns, this.values)
                }

                /**
                 * Returns `true` if the iteration has more elements.
                 */
                override fun hasNext(): Boolean = this.wrapped.hasNext()
            }
        }

        /**
         * Insert the provided [Record]. This will set this [DefaultEntity.Tx] to [TxStatus.DIRTY].
         *
         * @param record The [Record] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         *
         * @throws TxException If some of the [Tx] on [Column] or [Index] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        override fun insert(record: Record): TupleId? = this.withWriteLock {
            try {
                var lastTupleId: TupleId? = null
                val inserts = Object2ObjectArrayMap<ColumnDef<*>, Value>(this@DefaultEntity.columns.size)

                /* This is a critical section and requires a latch. */
                this@DefaultEntity.columns.values.forEach {
                    val tx = this.context.getTx(it) as ColumnTx<Value>
                    val value = record[it.columnDef]
                    val tupleId = tx.insert(value)
                    if (lastTupleId != tupleId && lastTupleId != null) {
                        throw DatabaseException.DataCorruptionException("Entity '${this@DefaultEntity.name}' is corrupt. Insert did not yield same record ID for all columns involved!")
                    }
                    lastTupleId = tupleId
                    inserts[it.columnDef] = value
                }

                /* Issue DataChangeEvent.InsertDataChange event and update indexes + statistics. */
                val event = DataChangeEvent.InsertDataChangeEvent(this@DefaultEntity, lastTupleId!!, inserts)
                this.snapshot.indexes.values.forEach { (this.context.getTx(it) as IndexTx).update(event) }
                this.snapshot.statistics.consume(event)
                this.context.signalEvent(event)

                return lastTupleId
            } catch (e: DatabaseException) {
                this.status = TxStatus.ERROR
                throw e
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Inserting record failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Updates the provided [Record] (identified based on its [TupleId]). Columns specified in the [Record] that are not part
         * of the [DefaultEntity] will cause an error! This will set this [DefaultEntity.Tx] to [TxStatus.DIRTY].
         *
         * @param record The [Record] that should be updated
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun update(record: Record) = this.withWriteLock {
            try {
                val updates = Object2ObjectArrayMap<ColumnDef<*>, Pair<Value?, Value?>>(record.columns.size)
                record.columns.forEach { def ->
                    val column = this@DefaultEntity.columns[def.name] ?: throw IllegalArgumentException("Column $def does not exist on entity ${this@DefaultEntity.name}.")
                    val value = record[def]
                    val columnTx = (this.context.getTx(column) as ColumnTx<Value>)
                    updates[def] = Pair(columnTx.update(record.tupleId, value), value) /* Map: ColumnDef -> Pair[Old, New]. */
                }

                /* Issue DataChangeEvent.UpdateDataChangeEvent and update indexes + statistics. */
                val event = DataChangeEvent.UpdateDataChangeEvent(this@DefaultEntity, record.tupleId, updates)
                this.snapshot.indexes.values.forEach { (this.context.getTx(it) as IndexTx).update(event) }
                this.snapshot.statistics.consume(event)
                this.context.signalEvent(event)
            } catch (e: DatabaseException) {
                this.status = TxStatus.ERROR
                throw e
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Updating record ${record.tupleId} failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Deletes the entry with the provided [TupleId]. This will set this [DefaultEntity.Tx] to [TxStatus.DIRTY]
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun delete(tupleId: TupleId) = this.withWriteLock {
            try {
                /* Perform delete on each column. */
                val deleted = Object2ObjectArrayMap<ColumnDef<*>, Value?>(this@DefaultEntity.columns.size)
                this@DefaultEntity.columns.values.map {
                    deleted[it.columnDef] = (this.context.getTx(it) as ColumnTx<*>).delete(tupleId)
                }

                /* Issue DataChangeEvent.DeleteDataChangeEvent and update indexes + statistics. */
                val event = DataChangeEvent.DeleteDataChangeEvent(this@DefaultEntity, tupleId, deleted)
                this.snapshot.indexes.values.forEach { (this.context.getTx(it) as IndexTx).update(event) }
                this.snapshot.statistics.consume(event)
                this.context.signalEvent(event)
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Deleting record $tupleId failed due to an error in the underlying storage: ${e.message}.")
            }
        }

        /**
         * Releases the [closeLock] on the [DefaultEntity].
         */
        override fun cleanup() {
            this@DefaultEntity.closeLock.unlockRead(this.closeStamp)
        }

        /**
         * A [TxAction] for creating a new [Index].
         *
         * @param index [Index] that has been created.
         */
        inner class CreateIndexTxAction(private val index: Index) : TxAction {
            override fun commit() {
                this.index.close()
                val move = Files.move(this.index.path, TxFileUtilities.plainPath(this.index.path), StandardCopyOption.ATOMIC_MOVE)
                this@DefaultEntity.indexes[this.index.name] = this.index.type.open(move, this@DefaultEntity)
            }

            override fun rollback() {
                this.index.close()
                TxFileUtilities.delete(this.index.path)
            }
        }

        /**
         * A [TxAction] implementation for dropping an [Index].
         *
         * @param index [Index] that has been dropped.
         */
        inner class DropIndexTxAction(private val index: Name.IndexName) : TxAction {
            override fun commit() {
                val index = this@DefaultEntity.indexes.remove(this.index) ?: throw IllegalStateException("Failed to drop index $index because it is unknown to the enclosing entity. This is a programmer's error!")
                index.close()
                if (Files.exists(index.path)) {
                    TxFileUtilities.delete(index.path)
                }
            }

            override fun rollback() {}
        }
    }
}
