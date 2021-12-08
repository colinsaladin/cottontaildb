package org.vitrivr.cottontail.database.column.mapdb

import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.mapdb.Serializer
import org.mapdb.Store
import org.vitrivr.cottontail.config.MapDBConfig
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.*
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.values.types.Value
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.locks.StampedLock

/**
 * Represents a [Column] in the Cottontail DB model that uses the Map DB storage engine.
 *
 * @see DefaultEntity
 *
 * @param <T> Type of the value held by this [MapDBColumn].
 *
 * @author Ralph Gasser
 * @version 2.0.1
 */
class MapDBColumn<T : Value>(override val path: Path, override val parent: Entity) : Column<T> {
    /**
     * Companion object with some important constants.
     */
    companion object {
        /**
         * The shift between Cottontail DB's [TupleId] and the record ID used by MapDB.
         *
         * Cottontail DB's [TupleId]'s are zero based.
         * MapDB's record ID is 1 based + the first record is reserved for the header.
         */
        private const val RECORD_ID_TUPLE_ID_SHIFT = 2L


        /** Record ID of the [ColumnHeader]. */
        private const val HEADER_RECORD_ID: Long = 1L

        /**
         * Initializes a new, empty [MapDBColumn]
         *
         * @param path The [Path] to the folder in which the [MapDBColumn] file should be stored.
         * @param column The [ColumnDef] that specifies the [MapDBColumn]
         * @param config The [MapDBConfig] used to initialize the [MapDBColumn]
         */
        fun initialize(path: Path, column: ColumnDef<*>, config: MapDBConfig) {
            if (Files.exists(path)) throw DatabaseException.InvalidFileException("Could not initialize column ${column.name}. A file already exists under $path.")
            val store = config.store(path)
            store.put(ColumnHeader(column), ColumnHeader.Serializer)
            store.commit()
            store.close()
        }
    }

    /** Internal reference to the [Store] underpinning this [MapDBColumn]. */
    private var store: CottontailStoreWAL = try {
        this.parent.parent.parent.config.mapdb.store(this.path)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open column at '$path': ${e.message}'")
    }

    /** Internal reference to the header of this [MapDBColumn]. */
    private val header
        get() = this.store.get(HEADER_RECORD_ID, ColumnHeader.Serializer)
            ?: throw DatabaseException.DataCorruptionException("Failed to open header of column '$name'!'")

    /** Getter for this [MapDBColumn]'s [ColumnDef]. Can be stored since [MapDBColumn]s [ColumnDef] is immutable. */
    @Suppress("UNCHECKED_CAST")
    override val columnDef: ColumnDef<T> = this.header.columnDef as ColumnDef<T>

    /** The [Name.ColumnName] of this [MapDBColumn]. Can be stored since [MapDBColumn]s [ColumnDef] is immutable. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** The [DBOVersion] of this [MapDBColumn]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** The [ColumnEngine] for [MapDBColumn]. */
    override val engine: ColumnEngine
        get() = ColumnEngine.MAPDB

    /** The maximum tuple ID used by this [Column]. */
    override val maxTupleId: Long
        get() = this.store.maxRecid

    /** Status indicating whether this [MapDBColumn] is open or closed. */
    override val closed: Boolean
        get() = this.store.isClosed

    /** An internal lock that is used to synchronize closing of the[MapDBColumn] in presence of ongoing [MapDBColumn.Tx]. */
    private val closeLock = StampedLock()

    /**
     * Creates and returns a new [MapDBColumn.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [MapDBColumn.Tx] for.
     * @return New [MapDBColumn.Tx]
     */
    override fun newTx(context: TransactionContext) = Tx(context)

    /**
     * Closes the [MapDBColumn]. Closing an [MapDBColumn] is a delicate matter since ongoing [MapDBColumn.Tx]  are involved.
     * Therefore, access to the method is mediated by an global [MapDBColumn] wide lock.
     */
    override fun close() {
        if (!this.closed) {
            this.store.close()
        }
    }

    /**
     * A [Tx] that affects this [MapDBColumn].
     *
     * @author Ralph Gasser
     * @version 1.4.0
     */
    inner class Tx constructor(context: TransactionContext) : AbstractTx(context), ColumnTx<T> {

        /** Reference to the [MapDBColumn] this [MapDBColumn.Tx] belongs to. */
        override val dbo: Column<T>
            get() = this@MapDBColumn

        /** The [Serializer] used for de-/serialization of [MapDBColumn] entries. */
        private val serializer = this@MapDBColumn.type.serializerFactory().mapdb(this@MapDBColumn.type.logicalSize)

        /** Obtains a global (non-exclusive) read-lock on [MapDBColumn]. Prevents enclosing [MapDBColumn] from being closed while this [MapDBColumn.Tx] is still in use. */
        private val closeStamp = this@MapDBColumn.closeLock.readLock()

        /** Tries to acquire a global read-lock on the surrounding column. */
        init {
            if (this@MapDBColumn.closed) {
                this@MapDBColumn.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this@MapDBColumn)
            }
        }

        /** [TxSnapshot] of this [ColumnTx]. */
        override val snapshot = object : ColumnTxSnapshot {
            @Volatile
            override var delta = 0L

            override val actions: List<TxAction> = emptyList()

            /** Commits the [ColumnTx] and integrates all changes made through it into the [MapDBColumn]. */
            override fun commit() {
                val header = this@MapDBColumn.header.copy(modified = System.currentTimeMillis(), count = (this@MapDBColumn.header.count + this.delta))
                this@MapDBColumn.store.update(HEADER_RECORD_ID, header, ColumnHeader.Serializer)
                this@MapDBColumn.store.commit()
            }

            /** Commits the [ColumnTx] and integrates all changes made through it into the [MapDBColumn]. */
            override fun rollback() {
                this@MapDBColumn.store.rollback()
            }

            override fun record(action: TxAction): Boolean = false
        }

        /**
         * Gets and returns an entry from this [MapDBColumn].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         *
         * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun read(tupleId: Long): T? = this.withReadLock {
            return this@MapDBColumn.store.get(tupleId + RECORD_ID_TUPLE_ID_SHIFT, this.serializer)
        }

        /**
         * Returns the number of entries in this [MapDBColumn]. Action acquires a global read dataLock for the [MapDBColumn].
         *
         * @return The number of entries in this [MapDBColumn].
         */
        override fun count(): Long = this.withReadLock {
            return (this@MapDBColumn.header.count + this.snapshot.delta)
        }

        /**
         * Creates and returns a new [Iterator] for this [MapDBColumn.Tx] that returns
         * all [TupleId]s contained within the surrounding [MapDBColumn].
         *
         * @return [Iterator]
         */
        override fun scan() = this.scan(0L..this@MapDBColumn.maxTupleId)

        /**
         * Creates and returns a new [Iterator] for this [MapDBColumn.Tx] that returns
         * all [TupleId]s contained within the surrounding [MapDBColumn] and a certain range.
         *
         * @param range The [LongRange] that should be scanned.
         * @return [Iterator]
         */
        override fun scan(range: LongRange) = this@Tx.withReadLock {
            object : Iterator<TupleId> {
                private val wrapped = this@MapDBColumn.store.RecordIdIterator((range.first + RECORD_ID_TUPLE_ID_SHIFT)..(range.last + RECORD_ID_TUPLE_ID_SHIFT))
                override fun hasNext(): Boolean = this.wrapped.hasNext()
                override fun next(): TupleId = this.wrapped.next() - RECORD_ID_TUPLE_ID_SHIFT
            }
        }

        /**
         * Inserts a new record in this [MapDBColumn]. This tasks will set this [MapDBColumn.Tx] to
         * [TxStatus.DIRTY] and acquire a column-wide write lock until the [MapDBColumn.Tx] either
         * commit or rollback is issued.
         *
         * @param record The record that should be inserted. Can be null!
         * @return The tupleId of the inserted record OR the allocated space in case of a null value.
         */
        override fun insert(record: T?): Long = this.withWriteLock {
            try {
                val tupleId = if (record == null && this.columnDef.nullable) {
                    this@MapDBColumn.store.preallocate() - RECORD_ID_TUPLE_ID_SHIFT
                } else if (record != null) {
                    this@MapDBColumn.store.put(record, this.serializer) - RECORD_ID_TUPLE_ID_SHIFT
                } else {
                    throw IllegalArgumentException("Column $columnDef does not allow for NULL values.")
                }

                /* Increment delta. */
                this.snapshot.delta += 1
                return tupleId
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw TxException.TxStorageException(this.context.txId, e.message ?: "Unknown")
            }
        }

        /**
         * Updates the entry with the specified tuple ID and sets it to the new value.
         *
         * This will set this [MapDBColumn.Tx] to [TxStatus.DIRTY] and acquire a column-wide
         * write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleId The ID of the record that should be updated
         * @param value The new [Value] or null.
         * @return The old [Value] or null
         */
        override fun update(tupleId: TupleId, value: T?): T? = this.withWriteLock {
            try {
                val ret = this@MapDBColumn.store.get(tupleId + RECORD_ID_TUPLE_ID_SHIFT, this.serializer)
                this@MapDBColumn.store.update(tupleId + RECORD_ID_TUPLE_ID_SHIFT, value, this.serializer)
                return ret
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw TxException.TxStorageException(this.context.txId, e.message ?: "Unknown exception during data storage")
            }
        }

        /**
         * Updates the entry with the specified tuple ID and sets it to the new value.
         *
         * This will set this [MapDBColumn.Tx] to [TxStatus.DIRTY] and acquire a column-wide
         * write lock until the [MapDBColumn.Tx] either commit or rollback is issued.
         *
         * @param tupleId The ID of the record that should be updated
         * @param value The new [Value].
         * @param expected The [Value] expected to be there.
         * @return True upon success, false otherwise.
         */
        override fun compareAndUpdate(tupleId: TupleId, value: T?, expected: T?): Boolean = this.withWriteLock {
            try {
                val ret = this@MapDBColumn.store.compareAndSwap(tupleId + RECORD_ID_TUPLE_ID_SHIFT, expected, value, this.serializer)
                return ret
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw TxException.TxStorageException(this.context.txId, e.message ?: "Unknown")
            }
        }

        /**
         * Deletes a record from this [MapDBColumn].
         *
         * This tasks will set this [MapDBColumn.Tx] to [TxStatus.DIRTY] and acquire a column-wide
         * write lock until the [MapDBColumn.Tx] either COMMIT or ROLLBACK is issued.
         *
         * @param tupleId The [TupleId] of the record that should be deleted
         * @return The old [Value]
         */
        override fun delete(tupleId: TupleId): T? = this.withWriteLock {
            try {
                val ret = this@MapDBColumn.store.get(tupleId + RECORD_ID_TUPLE_ID_SHIFT, this.serializer)
                this@MapDBColumn.store.delete(tupleId + RECORD_ID_TUPLE_ID_SHIFT, this.serializer)

                /* Decrement delta. */
                this.snapshot.delta -= 1
                this@MapDBColumn.store.update(HEADER_RECORD_ID, header, ColumnHeader.Serializer)

                return ret
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw TxException.TxStorageException(this.context.txId, e.message ?: "Unknown")
            }
        }

        /**
         * Releases the [closeLock] on the [MapDBColumn].
         */
        override fun cleanup() {
            this@MapDBColumn.closeLock.unlockRead(this.closeStamp)
        }
    }
}