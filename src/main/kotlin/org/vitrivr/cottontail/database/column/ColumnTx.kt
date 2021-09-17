package org.vitrivr.cottontail.database.column

import org.vitrivr.cottontail.database.general.Cursor
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Tx] that operates on a single [Column]. [Tx]s are a unit of isolation for data operations
 * (read/write).
 *
 * This interface defines the basic operations supported by such a [Tx]. However, it does not
 * dictate the isolation level. It is up to the implementation to define and implement the desired
 * level of isolation.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface ColumnTx<T : Value> : Tx {
    /** Reference to the [Column] this [ColumnTx] belongs to. */
    override val dbo: Column<T>

    /** The [ColumnDef] of the [Column] underlying this [ColumnTx]. */
    val columnDef: ColumnDef<T>
        get() = this.dbo.columnDef

    /**
     * Gets and returns [ValueStatistics] for this [ColumnTx]
     *
     * @return [ValueStatistics].
     */
    fun statistics(): ValueStatistics<T>

    /**
     * Opens a new [Cursor] for this [ColumnTx].
     *
     * @param start The [TupleId] to start the [Cursor] at.
     * @return [Cursor]
     */
    fun cursor(start: TupleId): Cursor<T>

    /**
     * Gets and returns an entry from this [Column].
     *
     * @param tupleId The ID of the desired entry
     * @return The desired entry.
     * @throws DatabaseException If the tuple with the desired ID is invalid.
     */
    fun get(tupleId: TupleId): T?

    /**
     * Updates the entry with the specified [TupleId] and sets it to the new [Value].
     *
     * @param tupleId The [TupleId] of the entry that should be updated.
     * @param value The new [Value]
     * @return The old [Value]
     */
    fun put(tupleId: TupleId, value: T): T?

    /**
     * Updates the entry with the specified [TupleId] and sets it to the new [Value] if, and only if, it currently holds the expected [Value].
     *
     * @param tupleId The ID of the record that should be updated
     * @param value The new [Value].
     * @param expected The [Value] expected to be there.
     */
    fun compareAndPut(tupleId: TupleId, value: T, expected: T?): Boolean

    /**
     * Deletes the entry with the specified [TupleId] and sets it to the new value.
     *
     * @param tupleId The ID of the record that should be updated
     * @return The old [Value]*
     */
    fun delete(tupleId: TupleId): T?
}