package org.vitrivr.cottontail.dbms.queries.operators.physical.merge

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.LimitTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sort.MergeLimitingHeapSortOperator
import org.vitrivr.cottontail.dbms.execution.operators.transform.MergeOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.LimitingSortPhysicalOperatorNode
import java.lang.Long.min

/**
 * An [NAryPhysicalOperatorNode] that represents the merging, sorting and limiting of inputs from different strands of execution.
 *
 * This particular operator only exists as physical operator and inherently implies potential for parallelism in an execution plan.
 * Since the collection of the different strands may potentially take place concurrently, the output order of  elements is usually
 * undefined. Furthermore, this operator must orchestrate a switching of [BindingContext] instances.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class MergeLimitingSortPhysicalOperatorNode(vararg inputs: Physical, val sortOn: List<Pair<ColumnDef<*>, SortOrder>>, val limit: Long): NAryPhysicalOperatorNode(*inputs) {

    /** Name of the [MergeLimitingSortPhysicalOperatorNode]. */
    override val name: String = "MergeLimitingSort"

    /** The output size of all [MergeLimitingSortPhysicalOperatorNode]s is usually the specified limit. */
    override val outputSize: Long
        get() = min(this.inputs.sumOf { it.outputSize }, this.limit)

    /** The [Cost] incurred by the [MergeLimitingSortPhysicalOperatorNode] is similar to that of the [LimitingSortPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(
             cpu = 2 * this.inputs.sumOf { it.outputSize } * this.sortOn.size * Cost.MEMORY_ACCESS.cpu,
             memory = (this.columns.sumOf {
                if (it.type == Types.String) {
                    this.statistics[it]!!.avgWidth * Char.SIZE_BYTES
                } else {
                    it.type.physicalSize
                }
        } * this.outputSize).toFloat()
    )

    /** The [Cost] incurred by merging is usually negligible. */
    override val parallelizableCost: Cost
        get() = this.inputs.map { it.totalCost }.reduce {c1, c2 -> c1 + c2}

    /** The [MergeLimitingSortPhysicalOperatorNode] overwrites/sets the [OrderTrait] and the [LimitTrait].  */
    override val traits: Map<TraitType<*>, Trait> = mapOf(OrderTrait to OrderTrait(this.sortOn), LimitTrait to LimitTrait(this.limit))

    /**
     * Creates and returns a copy of this [MergeLimitingSortPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [MergeLimitingSortPhysicalOperatorNode].
     */
    override fun copy() = MergeLimitingSortPhysicalOperatorNode(sortOn = this.sortOn, limit = this.limit)

    /**
     * Converts this [MergeLimitingSortPhysicalOperatorNode] to a [MergeOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = MergeLimitingHeapSortOperator(this.inputs.map { it.toOperator(ctx.split()) }, ctx.bindings, this.sortOn, this.limit)
}