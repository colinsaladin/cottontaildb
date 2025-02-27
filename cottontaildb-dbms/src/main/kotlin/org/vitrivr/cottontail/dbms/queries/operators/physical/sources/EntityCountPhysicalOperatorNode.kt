package org.vitrivr.cottontail.dbms.queries.operators.physical.sources

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.sources.EntityCountOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics

/**
 * A [NullaryPhysicalOperatorNode] that formalizes the counting entries in a physical [Entity].
 *
 * @author Ralph Gasser
 * @version 2.5.0
 */
class EntityCountPhysicalOperatorNode(override val groupId: Int, val entity: EntityTx, val out: Binding.Column) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "CountEntity"
    }

    /** The name of this [EntityCountPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The output size of an [EntityCountPhysicalOperatorNode] is always one. */
    override val outputSize = 1L

    /** physical [ColumnDef] accessed by this [EntityCountPhysicalOperatorNode]. */
    override val physicalColumns: List<ColumnDef<*>> = emptyList()

    /** [ColumnDef] produced by this [EntityCountPhysicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> = listOf(this.out.column)

    /** [EntityCountPhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    /** The estimated [Cost] of incurred by this [EntityCountPhysicalOperatorNode]. */
    override val cost = Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS

    /** [ValueStatistics] are taken from the underlying [Entity]. The query planner uses statistics for [Cost] estimation. */
    override val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>, ValueStatistics<*>>()

    /** The [EntityCountOperator] cannot be partitioned. */
    override val traits: Map<TraitType<*>, Trait> = mapOf(NotPartitionableTrait to NotPartitionableTrait)

    /**
     * Creates and returns a copy of this [EntityCountPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityCountPhysicalOperatorNode].
     */
    override fun copy() = EntityCountPhysicalOperatorNode(this.groupId, this.entity, this.out)

    /**
     * Converts this [EntityCountPhysicalOperatorNode] to a [EntityCountOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): EntityCountOperator {
        /* Bind all column bindings to context. */
        this.out.bind(ctx.bindings)

        /* Generate and return EntityCountOperator. */
        return EntityCountOperator(this.groupId, this.entity, this.out)
    }

    /** Generates and returns a [String] representation of this [EntityCountPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntityCountPhysicalOperatorNode) return false
        if (entity != other.entity) return false
        return true
    }

    override fun hashCode(): Int = this.entity.hashCode()
}