package org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.ColumnPair
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform.DistancePhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.functions.math.distance.Distances

/**
 * A [UnaryLogicalOperatorNode] that represents calculating the distance of a certain [ColumnDef] to a certain
 * query vector (expressed by a [KnnPredicate]).
 *
 * This can be used for late population, which can lead to optimized performance for kNN queries
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
@Deprecated("Replaced by FunctionProjectionLogicalOperator; do not use anymore!")
class DistanceLogicalOperatorNode(input: OperatorNode.Logical? = null, val predicate: KnnPredicate) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Distance"
    }

    /** The name of this [DistanceLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [DistanceLogicalOperatorNode] returns the [ColumnDef] of its input + a distance column. */
    override val columns: List<ColumnPair>
        get() = (this.input?.columns ?: emptyList()) + (Distances.DISTANCE_COLUMN_DEF to null)

    /** The [DistanceLogicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: List<ColumnDef<*>> = listOf(this.predicate.column)

    /**
     * Creates and returns a copy of this [LimitLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LimitLogicalOperatorNode].
     */
    override fun copy() = DistanceLogicalOperatorNode(predicate = this.predicate)

    /**
     * Returns a [DistancePhysicalOperatorNode] representation of this [DistanceLogicalOperatorNode]
     *
     * @return [DistancePhysicalOperatorNode]
     */
    override fun implement(): Physical = DistancePhysicalOperatorNode(this.input?.implement(), this.predicate)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DistanceLogicalOperatorNode) return false

        if (predicate != other.predicate) return false

        return true
    }

    /** Generates and returns a [String] representation of this [DistanceLogicalOperatorNode]. */
    override fun hashCode(): Int = this.predicate.hashCode()
}