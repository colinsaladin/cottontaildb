package org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.statistics.columns.DoubleValueStatistics
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.projection.DistanceProjectionOperator
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a [KnnPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
@Deprecated("Replaced by FunctionProjectionOperator; do not use anymore!")
class DistancePhysicalOperatorNode(input: Physical? = null, val predicate: KnnPredicate) : UnaryPhysicalOperatorNode(input) {
    companion object {
        private const val NODE_NAME = "Distance"
    }

    /** The name of this [DistancePhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [DistancePhysicalOperatorNode] returns the [ColumnDef] of its input + a distance column. */
    override val columns: List<ColumnDef<*>>
        get() = super.columns + KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())

    /** The [DistancePhysicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: List<ColumnDef<*>> = this.predicate.columns.toList()

    /** The [Cost] of a [DistancePhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.outputSize * this.predicate.atomicCpuCost)

    /** The [RecordStatistics] for this [DistanceProjectionOperator]. Contains an empty [DoubleValueStatistics] for the distance column. */
    override val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>,ValueStatistics<*>>()

    /** Whether the [DistanceProjectionOperator] can be partitioned is determined by the [KnnPredicateHint]. */
    override val canBePartitioned: Boolean
        get() = super.canBePartitioned

    init {
        this.statistics.putAll(this.input?.statistics ?: emptyMap())
        this.statistics[this.predicate.produces] = DoubleValueStatistics()
    }

    /**
     * Creates and returns a copy of this [DistancePhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [DistancePhysicalOperatorNode].
     */
    override fun copy() = DistancePhysicalOperatorNode(predicate = this.predicate)

    /**
     * Partitions this [DistancePhysicalOperatorNode]. The number of partitions can be override by a [KnnPredicateHint.ParallelKnnHint]
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> {
        val input = this.input ?: throw IllegalStateException("Cannot partition disconnected OperatorNode (node = $this)")
        return input.partition(p).map { DistancePhysicalOperatorNode(it, this.predicate) }
    }

    /**
     * Converts this [DistancePhysicalOperatorNode] to a [DistanceProjectionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return DistanceProjectionOperator(input, this.predicate.column, this.predicate.distance)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DistancePhysicalOperatorNode) return false

        if (predicate != other.predicate) return false

        return true
    }

    override fun hashCode(): Int {
        return predicate.hashCode()
    }
}