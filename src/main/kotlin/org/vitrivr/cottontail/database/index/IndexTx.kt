package org.vitrivr.cottontail.database.index

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.general.Cursor
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.index.basics.IndexConfig
import org.vitrivr.cottontail.database.index.basics.IndexState
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.database.operations.Operation
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.basics.Countable
import org.vitrivr.cottontail.model.basics.Filterable
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.TxException

/**
 * A [Tx] that operates on a single [Index]. [Tx]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface IndexTx : Tx, Filterable, Countable {

    /** Reference to the [Index] this [IndexTx] belongs to. */
    override val dbo: Index

    /** The [ColumnDef]s indexed by the [Index] this [IndexTx] belongs to. */
    val columns: Array<ColumnDef<*>>

    /** The order in which results of this [IndexTx] appear. Empty array that there is no particular order. */
    val order: Array<Pair<ColumnDef<*>, SortOrder>>

    /** The [IndexType] of the [Index] that underpins this [IndexTx]. */
    val state: IndexState

    /** The [IndexType] of the [Index] that underpins this [IndexTx]. */
    val type: IndexType

    /** The configuration map used for the [Index] that underpins this [IndexTx]. */
    val config: IndexConfig

    /**
     * (Re-)builds the underlying [Index] completely.
     *
     * @throws [TxException.TxValidationException] If rebuild of [Index] fails for some reason.
     */
    @Throws(TxException.TxValidationException::class)
    fun rebuild()

    /**
     * Updates the [Index] underlying this [IndexTx] based on the provided [Operation.DataManagementOperation].
     *
     * Not all [Index] implementations support incremental updates. Should be indicated by [IndexTransaction#supportsIncrementalUpdate()]
     *
     * @param event [Operation.DataManagementOperation] that should be processed.
     * @throws [TxException.TxValidationException] If update of [Index] fails for some reason.
     */
    @Throws(TxException.TxValidationException::class)
    fun update(event: Operation.DataManagementOperation)

    /**
     * Clears the [Index] underlying this [IndexTx] and removes all entries it contains.
     *
     * @throws [TxException.TxValidationException] If update of [Index] fails for some reason.
     */
    @Throws(TxException.TxValidationException::class)
    fun clear()

    /**
     * Performs a lookup through this [IndexTx] and returns a [Cursor] of all the [Record]s that match the [Predicate].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @return The resulting [Cursor].
     */
    override fun filter(predicate: Predicate): Cursor<Record>

    /**
     * Performs a lookup through this [IndexTx] and returns a [Cursor] of all the [Record]s that match the [Predicate]
     * and fall within the specified data [LongRange], which must lie in 0..[count].
     *
     * Not all [Index] implementations support range filtering.
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param partitionIndex The [partitionIndex] for this [filterRange] call.
     * @param partitions The total number of partitions for this [filterRange] call.
     * @return The resulting [Cursor].
     */
    fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Cursor<Record>
}