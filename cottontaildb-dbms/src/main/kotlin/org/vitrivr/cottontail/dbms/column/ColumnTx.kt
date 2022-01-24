package org.vitrivr.cottontail.dbms.column

import org.vitrivr.cottontail.core.basics.Countable
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.Tx

/**
 * A [Tx] that operates on a single [Column]. [Tx]s are a unit of isolation for data operations
 * (read/write).
 *
 * This interface defines the basic operations supported by such a [Tx]. However, it does not
 * dictate the isolation level. It is up to the implementation to define and implement the desired
 * level of isolation.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
interface ColumnTx<T : Value> : Tx, Countable {
    /** Reference to the [Column] this [ColumnTx] belongs to. */
    override val dbo: Column<T>

    /** The [ColumnDef] of the [Column] underlying this [ColumnTx]. */
    val columnDef: ColumnDef<T>
        get() = this.dbo.columnDef

    /**
     * Gets and returns an entry from this [Column].
     *
     * @param tupleId The ID of the desired entry
     * @return The desired entry.
     *
     * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
     */
    fun read(tupleId: TupleId): T?

    /**
     * Inserts a new [Value] in this [Column].
     *
     * @param record The [Value] that should be inserted. Can be null!
     * @return The [TupleId] of the inserted record OR the allocated space in case of a null value.
     */
    fun insert(record: T?): TupleId

    /**
     * Updates the entry with the specified [TupleId] and sets it to the new [Value].
     *
     * @param tupleId The [TupleId] of the entry that should be updated.
     * @param value The new [Value]
     * @return The old [Value]
     */
    fun update(tupleId: TupleId, value: T?): T?

    /**
     * Updates the entry with the specified [TupleId] and sets it to the new [Value] if, and only if,
     * it currently hold the expected [Value].
     *
     * @param tupleId The ID of the record that should be updated
     * @param value The new [Value].
     * @param expected The [Value] expected to be there.
     */
    fun compareAndUpdate(tupleId: TupleId, value: T?, expected: T?): Boolean

    /**
     * Deletes the entry with the specified [TupleId] and sets it to the new value.
     *
     * @param tupleId The ID of the record that should be updated
     * @return The old [Value]*
     */
    fun delete(tupleId: TupleId): T?

    /**
     * Creates and returns a new [Iterator] for this [ColumnTx] that returns all
     * [TupleId]s contained within the surrounding [Column].
     *
     * @return [Iterator]
     */
    fun scan(): Iterator<TupleId>

    /**
     * Creates and returns a new [Iterator] for this [ColumnTx] that returns
     * all [TupleId]s contained within the surrounding [Column] and a certain range.
     *
     * @param range The [LongRange] that should be scanned.
     * @return [Iterator]
     */
    fun scan(range: LongRange): Iterator<TupleId>
}