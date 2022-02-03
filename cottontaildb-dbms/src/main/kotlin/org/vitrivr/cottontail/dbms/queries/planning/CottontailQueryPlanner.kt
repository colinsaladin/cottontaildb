package org.vitrivr.cottontail.dbms.queries.planning

import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.NAryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.BinaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule
import java.util.*

/**
 * This is a rather simple query planner that optimizes a [OperatorNode] by recursively applying a set of [RewriteRule]s to get more
 * sophisticated yet equivalent [OperatorNode]s. Query planning & optimization takes place in three stages:
 *
 * 1. The logical tree is rewritten by means of other [OperatorNode.Logical]s, to generate several, equivalent representations of the query.
 * 2. The candidate trees are "implemented", i.e., a physical tree is created for each logical tree .
 * 3. The physical tree is rewritten by replacing [OperatorNode.Physical]s by [OperatorNode.Physical] to arrive at an executable query plan.
 *
 * Finally, the best plan in terms of [Cost] is selected.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class CottontailQueryPlanner(private val logicalRules: Collection<RewriteRule>, private val physicalRules: Collection<RewriteRule>, planCacheSize: Int = 100) {

    /** Internal cache used to store query plans for known queries. */
    private val planCache = CottontailPlanCache(planCacheSize)

    /**
     * Executes query planning for a given [QueryContext] and generates a [OperatorNode.Physical] for it.
     *
     * @param context The [QueryContext] to plan for.
     * @param bypassCache If the plan cache should be bypassed (forces new planning).
     *
     * @throws QueryException.QueryPlannerException If planner fails to generate a valid execution plan.
     */
    fun planAndSelect(context: QueryContext, bypassCache: Boolean = false, cache: Boolean = false) {
        /* Try to obtain PhysicalNodeExpression from plan cache, unless bypassCache has been set to true. */
        val logical = context.logical
        require(logical != null) { "Cannot plan for a QueryContext that doesn't have a valid logical query representation." }
        val digest = logical.digest()
        if (!bypassCache) {
            context.physical = this.planCache[digest]
            if (context.physical != null) {
                return
            }
        }

        /* Execute actual query planning and select candidate with lowest cost. */
        val candidates = this.plan(context)
        context.physical = candidates.minByOrNull { it.totalCost } ?: throw QueryException.QueryPlannerException("Failed to generate a physical execution plan for expression: $logical.")

        /* Update plan cache. */
        if (!cache) this.planCache[digest] = context.physical!!
    }

    /**
     * Generates a list of equivalent [OperatorNode.Physical]s by recursively applying [RewriteRule]s
     * on the seed [OperatorNode.Logical] and derived [OperatorNode]s.
     *
     * @param context The [QueryContext] to plan for.
     *
     * @return List of [OperatorNode.Physical] that implement the [OperatorNode.Logical]
     */
    fun plan(context: QueryContext): Collection<org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical> {
        val logical = context.logical
        require(logical != null) { QueryException.QueryPlannerException("Cannot perform query planning for a QueryContext that doesn't have a logical query plan.") }

        val decomposition = this.decompose(logical)
        val candidates = Int2ObjectLinkedOpenHashMap<OperatorNode.Physical>()
        for (d in decomposition) {
            val stage1 = this.optimizeLogical(d.value, context).map { it.implement() }
            val stage2 = stage1.flatMap { this.optimizePhysical(it, context) }
            val candidate = stage2.filter {
                it.executable /* Filter-out plans that cannot be executed. */
            }.minByOrNull {
                it.totalCost
            } ?: throw QueryException.QueryPlannerException("Failed to generate a physical execution plan for expression: $logical.")
            candidates[d.key] = candidate
        }
        return listOf(this.compose(0, candidates))
    }

    /**
     * Decomposes the given [OperatorNode.Logical], i.e., splits the tree into a sub-tree per group.
     * This is a preparation for query optimisation.
     *
     * @param operator [OperatorNode.Logical] That should be decomposed.
     */
    private fun decompose(operator: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical): Map<Int, org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical> {
        val decomposition = Int2ObjectLinkedOpenHashMap<org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical>()
        decomposition[operator.groupId] = operator.copyWithGroupInputs()
        var next: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical? = operator
        var prev: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical? = null
        while (next != null) {
            prev = next
            when (next) {
                is NullaryLogicalOperatorNode -> return decomposition
                is UnaryLogicalOperatorNode -> {
                    next = next.input
                }
                is org.vitrivr.cottontail.dbms.queries.operators.logical.BinaryLogicalOperatorNode -> {
                    if (next.right != null) decomposition.putAll(this.decompose(next.right!!))
                    next = next.left
                }
                is NAryLogicalOperatorNode -> {
                    next.inputs.drop(1).forEach { decomposition.putAll(this.decompose(it)) }
                    next = next.inputs.first()
                }
            }
        }
        throw IllegalStateException("Tree decomposition failed. Encountered null node while scanning tree (node = $prev). This is a programmer's error!")
    }

    /**
     * Composes a decomposition [Map] into a [OperatorNode.Physical], i.e., splits the tree into a sub-tree per group.
     * This is a preparation for query optimisation.
     *
     * @param decomposition The decomposition [Map] that should be recomposed.
     */
    private fun compose(startGroupId: GroupId = 0, decomposition: Map<Int, org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical>): org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical {
        val main = decomposition[startGroupId] ?: throw IllegalStateException("Tree composition failed. No entry for desired groupId $startGroupId.")
        var next: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical? = main
        var prev: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical? = null
        while (next != null) {
            prev = next
            when (next) {
                is NullaryPhysicalOperatorNode -> return main
                is UnaryPhysicalOperatorNode -> {
                    next = next.input
                }
                is BinaryPhysicalOperatorNode -> {
                    next.right = this.compose(startGroupId + 1, decomposition)
                    next = next.left
                }
                is NAryPhysicalOperatorNode -> {
                    repeat(next.inputArity - 1) {
                        (next as NAryPhysicalOperatorNode).addInput(this.compose(startGroupId + it + 1, decomposition))
                    }
                    next = next.inputs.first()
                }
            }
        }
        throw IllegalStateException("Tree composition failed. Encountered null node while scanning tree (node = $prev). This is a programmer's error!")
    }

    /**
     * Performs logical optimization, i.e., by replacing parts in the logical execution plan.
     *
     * @param operator The [OperatorNode.Logical] that should be optimized. Optimization starts from the given node, regardless of whether it is root or not.
     * @param ctx The [QueryContext] used for optimization.
     */
    private fun optimizeLogical(operator: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical, ctx: QueryContext): List<org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical> {
        /* List of candidates, objects to explore and digests */
        val candidates = MemoizingOperatorList(operator)
        val explore = MemoizingOperatorList(operator)

        /* Now start exploring... */
        var pointer = explore.dequeue()
        while (pointer != null) {
            /* Apply rules to node and add results to list for exploration. */
            for (rule in this.logicalRules) {
                val result = rule.apply(pointer, ctx)
                if (result is org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical) {
                    explore.enqueue(result)
                    candidates.enqueue(result)
                }
            }

            /* Add all inputs to operators that need further exploration. */
            when (pointer) {
                is NAryLogicalOperatorNode -> explore.enqueue(pointer.inputs.firstOrNull() ?: throw IllegalStateException("Encountered null node in logical operator node tree (node = $pointer). This is a programmer's error!"))
                is org.vitrivr.cottontail.dbms.queries.operators.logical.BinaryLogicalOperatorNode -> explore.enqueue(pointer.left ?: throw IllegalStateException("Encountered null node in logical operator node tree (node = $pointer). This is a programmer's error!"))
                is UnaryLogicalOperatorNode -> explore.enqueue(pointer.input ?: throw IllegalStateException("EEncountered null node in logical operator node tree (node = $pointer). This is a programmer's error!"))
                is org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical -> throw IllegalStateException("Encountered physical operator node in logical operator node tree. This is a programmer's error!")
            }

            /* Get next in line. */
            pointer = explore.dequeue()
        }
        return candidates.toList()
    }

    /**
     * Performs physical optimization, i.e., by replacing parts in the physical execution plan.
     *
     * @param operator The [OperatorNode.Physical] that should be optimized. Optimization starts from the given node, regardless of whether it is root or not.
     * @param ctx The [QueryContext] used for optimization.
     */
    private fun optimizePhysical(operator: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical, ctx: QueryContext): List<org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical> {
        /* List of candidates, objects to explore and digests */
        val candidates = MemoizingOperatorList(operator.root)
        val explore = MemoizingOperatorList(operator.root)

        /* Now start exploring... */
        var pointer = explore.dequeue()
        while (pointer != null) {
            /* Apply rules to node and add results to list for exploration. */
            for (rule in this.physicalRules) {
                val result = rule.apply(pointer, ctx)
                if (result is org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Physical) {
                    explore.enqueue(result)
                    candidates.enqueue(result)
                }
            }

            /* Add all inputs to operators that need further exploration. */
            when (pointer) {
                is NAryPhysicalOperatorNode -> explore.enqueue(pointer.inputs.firstOrNull() ?: throw IllegalStateException("Encountered null node in physical operator node tree (node = $pointer). This is a programmer's error!"))
                is BinaryPhysicalOperatorNode -> explore.enqueue(pointer.left ?: throw IllegalStateException("Encountered null node in physical operator node tree (node = $pointer). This is a programmer's error!"))
                is UnaryPhysicalOperatorNode -> explore.enqueue(pointer.input ?: throw IllegalStateException("Encountered null node in physical operator node tree (node = $pointer). This is a programmer's error!"))
                is org.vitrivr.cottontail.dbms.queries.operators.OperatorNode.Logical -> throw IllegalStateException("Encountered logical operator node in physical operator node tree. This is a programmer's error!")
            }

            /* Get next in line. */
            pointer = explore.dequeue()
        }
        return candidates.toList()
    }
}