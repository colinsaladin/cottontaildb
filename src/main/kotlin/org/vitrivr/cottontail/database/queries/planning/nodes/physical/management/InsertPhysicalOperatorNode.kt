package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.ColumnPair
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.InsertLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [InsertPhysicalOperatorNode] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class InsertPhysicalOperatorNode(override val groupId: GroupId, val entity: EntityTx, val records: MutableList<Record>) : NullaryPhysicalOperatorNode() {
    companion object {
        private const val NODE_NAME = "Insert"
    }

    /** The name of this [InsertPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [InsertPhysicalOperatorNode] produces the [ColumnPair]s defined in the [InsertOperator]. */
    override val columns: List<ColumnPair> = InsertOperator.COLUMNS

    /** The [ValueStatistics] for this [InsertPhysicalOperatorNode]. */
    override val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>,ValueStatistics<*>>()

    /** The [InsertPhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    /** The [Cost] of this [InsertPhysicalOperatorNode]. */
    override val cost: Cost = Cost(Cost.COST_DISK_ACCESS_WRITE, Cost.COST_MEMORY_ACCESS) * this.records.size

    /** The [InsertPhysicalOperatorNode] cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /** The [InsertPhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    init {
        /* Obtain statistics costs. */
        this.entity.listColumns().forEach { columnDef ->
            this.statistics[columnDef] = (this.entity.context.getTx(this.entity.columnForName(columnDef.name)) as ColumnTx<*>).statistics() as ValueStatistics<Value>
        }
    }

    /**
     * Creates and returns a copy of this [InsertLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [InsertLogicalOperatorNode].
     */
    override fun copy() = InsertPhysicalOperatorNode(this.groupId, this.entity, this.records)

    /**
     * Converts this [InsertPhysicalOperatorNode] to a [InsertOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = InsertOperator(this.groupId, this.entity, this.records)

    /**
     * [InsertPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("InsertPhysicalOperatorNode cannot be partitioned.")
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is InsertPhysicalOperatorNode) return false

        if (entity != other.entity) return false
        if (records != other.records) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + records.hashCode()
        return result
    }
}