package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.AbstractIndex
import org.vitrivr.cottontail.database.queries.*
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.database.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sort.MergeLimitingHeapSortOperator
import org.vitrivr.cottontail.execution.operators.sources.IndexScanOperator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [IndexScanPhysicalOperatorNode] that represents a predicated lookup using an [AbstractIndex].
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class IndexScanPhysicalOperatorNode(override val groupId: Int, val index: IndexTx, val predicate: Predicate, val fetch: List<Pair<Name.ColumnName,ColumnDef<*>>>) : NullaryPhysicalOperatorNode() {
    companion object {
        private const val NODE_NAME = "ScanIndex"
    }

    /** The name of this [IndexScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [ColumnDef]s produced by this [IndexScanPhysicalOperatorNode] depends on the [ColumnDef]s produced by the [Index]. */
    override val columns: List<ColumnPair> = this.fetch.map {
        require(this.index.dbo.produces.contains(it.second)) { "The given column $it is not produced by the selected index ${this.index.dbo}. This is a programmer's error!"}
        it.second.copy(name = it.first) to it.second /* Logical --> physical column. */
    }

    /** [IndexScanPhysicalOperatorNode] are always executable. */
    override val executable: Boolean = true

    /** Whether an [IndexScanPhysicalOperatorNode] can be partitioned depends on the [Index]. */
    override val canBePartitioned: Boolean = this.index.dbo.supportsPartitioning

    /** [ValueStatistics] are taken from the underlying [Entity]. The query planner uses statistics for [Cost] estimation. */
    override val statistics by lazy {
        val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>,ValueStatistics<*>>()
        val entityTx = this.index.context.getTx(this.index.dbo.parent) as EntityTx
        this.columns.forEach {
            val physical = it.physical()
            if (physical != null && entityTx.listColumns().contains(physical)) {
                statistics[it.logical()] = (this.index.context.getTx(entityTx.columnForName(physical.name)) as ColumnTx<*>).statistics() as ValueStatistics<Value>
            }
        }
        statistics
    }

    /** The number of rows returned by this [IndexScanPhysicalOperatorNode]. This is usually an estimate! */
    override val outputSize: Long
        get() = when (this.predicate) {
            is BooleanPredicate -> NaiveSelectivityCalculator.estimate(this.predicate, this.statistics)(this.index.dbo.parent.numberOfRows)
            is KnnPredicate -> this.predicate.k.toLong()
            else -> this.index.dbo.parent.numberOfRows
        }

    /** Cost estimation for [IndexScanPhysicalOperatorNode]s is delegated to the [Index]. */
    override val cost: Cost by lazy { this.index.dbo.cost(this.predicate) }
    /**
     * Creates and returns a copy of this [IndexScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [IndexScanPhysicalOperatorNode].
     */
    override fun copy() = IndexScanPhysicalOperatorNode(this.groupId, this.index, this.predicate, this.fetch)

    /**
     * Partitions this [IndexScanPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        check(this.index.dbo.supportsPartitioning) { "Index ${index.dbo.name} does not support partitioning!" }
        return (0 until p).map {
            RangedIndexScanPhysicalOperatorNode(this.groupId, this.index, this.predicate, this.fetch, it, p)
        }
    }

    /**
     * Converts this [IndexScanPhysicalOperatorNode] to a [IndexScanOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = when (this.predicate) {
        is KnnPredicate -> {
            val p = this.totalCost.parallelisation()
            if (p > 1 && this.canBePartitioned) {
                val partitions = this.partition(p)
                val operators = partitions.map { it.toOperator(ctx) }
                MergeLimitingHeapSortOperator(operators, listOf(Pair(this.predicate.produces, SortOrder.ASCENDING)), this.predicate.k.toLong())
            } else {
                IndexScanOperator(this.groupId, this.index, this.predicate, this.fetch, ctx.bindings)
            }
        }
        is BooleanPredicate -> IndexScanOperator(this.groupId, this.index, this.predicate, this.fetch, ctx.bindings)
        else -> throw UnsupportedOperationException("Unknown type of predicate ${this.predicate} cannot be converted to operator.")
    }

    /** Generates and returns a [String] representation of this [EntityScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.index.type},${this.predicate}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IndexScanPhysicalOperatorNode) return false

        if (depth != other.depth) return false
        if (predicate != other.predicate) return false

        return true
    }

    override fun hashCode(): Int {
        var result = depth.hashCode()
        result = 31 * result + predicate.hashCode()
        return result
    }
}