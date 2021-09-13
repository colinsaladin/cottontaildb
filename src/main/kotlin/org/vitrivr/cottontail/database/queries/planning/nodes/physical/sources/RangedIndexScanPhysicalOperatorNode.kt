package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.database.statistics.entity.EntityStatistics
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.IndexScanOperator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Math.floorDiv

/**
 * A [NullaryPhysicalOperatorNode] that formalizes a scan of a physical [Index] in Cottontail DB on a given range.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class RangedIndexScanPhysicalOperatorNode(override val groupId: Int, val index: IndexTx, val predicate: Predicate, val fetch: List<Pair<Name.ColumnName,ColumnDef<*>>>, val partitionIndex: Int, val partitions: Int) : NullaryPhysicalOperatorNode() {
    companion object {
        private const val NODE_NAME = "ScanIndex"
    }

    /** The name of this [RangedIndexScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME


    override val outputSize: Long
    override val statistics: RecordStatistics
    override val columns: List<ColumnDef<*>> = this.fetch.map {
        require(this.index.dbo.produces.contains(it.second)) { "The given column $it is not produec by the selected index ${this.index.dbo}. This is a programmer's error!"}
        it.second.copy(name = it.first)
    }
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = false
    override val cost = this.index.dbo.cost(this.predicate)

    init {
        require(this.partitionIndex >= 0) { "The partitionIndex of a ranged index scan must be greater than zero." }
        require(this.partitions > 0) { "The number of partitions for a ranged index scan must be greater than zero." }

        /* Obtain statistics. */
        val entityTx = this.index.context.getTx(this.index.dbo.parent) as EntityTx
        this.statistics = EntityStatistics(entityTx.count(), entityTx.maxTupleId())
        entityTx.listColumns().forEach { columnDef ->
            this.statistics[columnDef] = (this.index.context.getTx(entityTx.columnForName(columnDef.name)) as ColumnTx<*>).statistics() as ValueStatistics<Value>
        }
        this.outputSize = floorDiv(this.statistics.count, this.partitions)
    }

    /**
     * Creates and returns a copy of this [RangedIndexScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [RangedIndexScanPhysicalOperatorNode].
     */
    override fun copy() = RangedIndexScanPhysicalOperatorNode(this.groupId, this.index, this.predicate, this.fetch, this.partitionIndex, this.partitions)

    /**
     * Converts this [RangedIndexScanPhysicalOperatorNode] to a [IndexScanOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = IndexScanOperator(this.groupId, this.index, this.predicate, this.fetch, this.partitionIndex, this.partitions)

    /**
     * [RangedIndexScanPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("RangedIndexScanPhysicalOperatorNode cannot be further partitioned.")
    }

    /** Generates and returns a [String] representation of this [RangedIndexScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.index.type},${this.predicate},${this.partitionIndex}/${this.partitions}/]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RangedIndexScanPhysicalOperatorNode) return false

        if (this.depth != other.depth) return false
        if (this.predicate != other.predicate) return false
        if (this.partitionIndex != other.partitionIndex) return false
        if (this.partitions != other.partitions) return false

        return true
    }

    override fun hashCode(): Int {
        var result = depth.hashCode()
        result = 31 * result + predicate.hashCode()
        result = 31 * result + partitionIndex.hashCode()
        result = 31 * result + partitions.hashCode()
        return result
    }
}