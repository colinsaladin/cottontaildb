package org.vitrivr.cottontail.legacy.v1.column

import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.column.ColumnEngine
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TxException
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.general.TxAction
import org.vitrivr.cottontail.dbms.general.TxSnapshot
import org.vitrivr.cottontail.storage.serializers.ValueSerializerFactory
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Represents a single column in the Cottontail DB model. A [ColumnV1] record is identified by a tuple ID (long)
 * and can hold an arbitrary value. Usually, multiple [ColumnV1]s make up an [Entity].
 *
 * @see Entity
 *
 * @param <T> Type of the value held by this [ColumnV1].
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class ColumnV1<T : Value>(
    override val name: Name.ColumnName,
    override val parent: Entity
) : Column<T> {

    /**
     * Companion object with some important constants.
     */
    companion object {
        /** Record ID of the [ColumnV1Header]. */
        private const val HEADER_RECORD_ID: Long = 1L
    }

    /** The [Path] to the [Entity]'s main folder. */
    override val path: Path = parent.path.resolve("col_${name.simple}.db")

    /** Internal reference to the [Store] underpinning this [ColumnV1]. */
    private var store: CottontailStoreWAL = try {
        this.parent.parent.parent.config.mapdb.store(this.path)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open column at '$path': ${e.message}'")
    }

    /** Internal reference to the [Header] of this [ColumnV1]. */
    private val header
        get() = this.store.get(HEADER_RECORD_ID, ColumnV1Header.Serializer)
            ?: throw DatabaseException.DataCorruptionException("Failed to open header of column '$name'!'")

    /**
     * Getter for this [ColumnV1]'s [ColumnDef]. Can be stored since [ColumnV1]s [ColumnDef] is immutable.
     *
     * @return [ColumnDef] for this [ColumnV1]
     */
    @Suppress("UNCHECKED_CAST")
    override val columnDef: ColumnDef<T> =
        this.header.let { ColumnDef(this.name, it.type as Types<T>, it.nullable) }

    /** The [DBOVersion] of this [ColumnV1]. */
    override val version: DBOVersion
        get() = DBOVersion.V1_0

    /** The [ColumnEngine] of this [ColumnV1]. */
    override val engine: ColumnEngine
        get() = ColumnEngine.MAPDB

    /**
     * The maximum tuple ID used by this [Column].
     */
    override val maxTupleId: Long
        get() = this.store.maxRecid

    /**
     * Status indicating whether this [ColumnV1] is open or closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /** An internal lock that is used to synchronize structural changes to an [ColumnV1] (e.g. closing or deleting) with running [ColumnV1.Tx]. */
    private val closeLock = StampedLock()

    /**
     * Closes the [ColumnV1]. Closing an [ColumnV1] is a delicate matter since ongoing [ColumnV1.Tx]
     * are involved. Therefore, access to the method is mediated by an global [ColumnV1] wide lock.
     */
    override fun close() = this.closeLock.write {
        this.store.close()
        this.closed = true
    }

    /**
     * Creates a new [ColumnV1.Tx] and returns it.
     *
     * @param readonly True, if the resulting [ColumnV1.Tx] should be a read-only transaction.
     * @param tid The ID for the new [ColumnV1.Tx]
     *
     * @return A new [ColumnTransaction] object.
     */
    override fun newTx(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): ColumnTx<T> = Tx(context)

    /**
     * A [Transaction] that affects this [ColumnV1].
     */
    inner class Tx constructor(override val context: org.vitrivr.cottontail.dbms.execution.TransactionContext) : AbstractTx(context), ColumnTx<T> {
        /**
         * The [ColumnDef] of the [Column] underlying this [ColumnTransaction].
         *
         * @return [ColumnTransaction]
         */
        override val columnDef: ColumnDef<T>
            get() = this@ColumnV1.columnDef

        override val dbo: Column<T>
            get() = this@ColumnV1

        /** The [Serializer] used for de-/serialization of [ColumnV1] entries. */
        private val serializer = ValueSerializerFactory.mapdb(this@ColumnV1.type)

        /** Obtains a global (non-exclusive) read-lock on [ColumnV1]. Prevents enclosing [ColumnV1] from being closed while this [ColumnV1.Tx] is still in use. */
        private val closeStamp = this@ColumnV1.closeLock.readLock()

        /** Tries to acquire a global read-lock on the [ColumnV1]. */
        init {
            if (this@ColumnV1.closed) {
                this@ColumnV1.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this@ColumnV1)
            }
        }

        /** The [TxSnapshot] for this [ColumnV1.Tx] */
        override val snapshot: TxSnapshot = object : TxSnapshot {
            override val actions: List<TxAction> = emptyList()
            override fun commit() = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
            override fun rollback() = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
            override fun record(action: TxAction): Boolean = throw UnsupportedOperationException("Operation not supported on legacy DBO.")

        }

        /**
         * Gets and returns an entry from this [ColumnV1].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         *
         * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun read(tupleId: Long): T? = this.withReadLock {
            this@ColumnV1.store.get(tupleId, this.serializer)
        }

        /**
         * Returns the number of entries in this [ColumnV1]. Action acquires a global read dataLock for the [ColumnV1].
         *
         * @return The number of entries in this [ColumnV1].
         */
        override fun count(): Long = this.withReadLock {
            this@ColumnV1.header.count
        }

        /**
         * Creates and returns a new [Iterator] for this [ColumnV1.Tx] that returns
         * all [TupleId]s contained within the surrounding [ColumnV1].
         *
         * @return [Iterator]
         */
        override fun scan() = this.scan(1L..this@ColumnV1.maxTupleId)

        /**
         * Creates and returns a new [Iterator] for this [ColumnV1.Tx] that returns
         * all [TupleId]s contained within the surrounding [ColumnV1] and a certain range.
         *
         * @param range The [LongRange] that should be scanned.
         * @return [Iterator]
         */
        override fun scan(range: LongRange) = this.withReadLock {
            object : Iterator<TupleId> {

                /** Wraps a [RecordIdIterator] from the [ColumnV1]. */
                private val wrapped = this@ColumnV1.store.RecordIdIterator(range)

                /**
                 * Returns the next element in the iteration.
                 */
                override fun next(): TupleId {
                    return this.wrapped.next()
                }

                /**
                 * Returns `true` if the iteration has more elements.
                 */
                override fun hasNext(): Boolean {
                    return this.wrapped.hasNext()
                }
            }
        }

        override fun insert(record: T?): TupleId {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun update(tupleId: Long, value: T?): T? {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun compareAndUpdate(tupleId: Long, value: T?, expected: T?): Boolean {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun delete(tupleId: Long): T? {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun cleanup() {
            this@ColumnV1.closeLock.unlockRead(this.closeStamp)
        }
    }
}