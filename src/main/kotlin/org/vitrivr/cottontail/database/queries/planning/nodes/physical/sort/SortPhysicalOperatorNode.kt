package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.logical
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s. Internally,
 * a heap sort algorithm is applied for sorting.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class SortPhysicalOperatorNode(input: Physical? = null, override val sortOn: List<Pair<ColumnDef<*>, SortOrder>>) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Order"
    }

    /** The name of this [SortPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [SortPhysicalOperatorNode] requires all [ColumnDef]s used on the ORDER BY clause. */
    override val requires: List<ColumnDef<*>> = this.sortOn.map { it.first }

    /** The [Cost] incurred by this [SortPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(
            cpu = 2 * this.sortOn.size * Cost.COST_MEMORY_ACCESS,
            memory = this.columns.sumOf { this.statistics[it.logical()]?.avgWidth ?: it.logical().type.logicalSize }.toFloat()
        ) * this.outputSize

    init {
        if (this.sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /**
     * Creates and returns a copy of this [SortPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [SortPhysicalOperatorNode].
     */
    override fun copy() = SortPhysicalOperatorNode(sortOn = this.sortOn)

    /**
     * Partitions this [SortPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> =
        this.input?.partition(p)?.map { SortPhysicalOperatorNode(it, this.sortOn) } ?: throw IllegalStateException("Cannot partition disconnected OperatorNode (node = $this)")

    /**
     * Converts this [SortPhysicalOperatorNode] to a [HeapSortOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = HeapSortOperator(
        this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)"),
        this.sortOn,
        if (this.outputSize > Integer.MAX_VALUE) {
            Integer.MAX_VALUE
            /** TODO: This case requires special handling. */
        } else {
            this.outputSize.toInt()
        }
    )

    /** Generates and returns a [String] representation of this [SortPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.sortOn.joinToString(",") { "${it.first.name} ${it.second}" }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SortPhysicalOperatorNode) return false
        if (this.sortOn != other.sortOn) return false
        return true
    }

    override fun hashCode(): Int = this.sortOn.hashCode()
}