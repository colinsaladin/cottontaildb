package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.simd

import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.ternary.WeightedVectorDistance
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.RewriteRule

/**
 * A [RewriteRule] that will be used to decide in which cases the usage of vectorized distance functions is beneficial.
 * The rule will be based on empiric data measurements.
 *
 * @author Colin Saladin
 * @version 1.0.0
 */
class SIMDRule(val catalogue: Catalogue) : RewriteRule {

    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is FunctionPhysicalOperatorNode && (node.function.arguments[0] as Binding.Function).function is WeightedVectorDistance<*, *>

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        // TODO @Colin - Optimize rule based on the performance evaluation
        if (node is FunctionPhysicalOperatorNode && (node.function.arguments[0] as Binding.Function).function is WeightedVectorDistance<*, *>) {
            val input = node.input?.copy() ?: return null
            val out = node.out

            val bindFunction = out.context.bind(((node.function.arguments[0] as Binding.Function).function as WeightedVectorDistance<*, *>).vectorized(), node.function.arguments)

            // Provisional heuristic
            if (((node.function.arguments[0] as Binding.Function).function as WeightedVectorDistance<*, *>).d >= 0) {
                val p = FunctionPhysicalOperatorNode(input as OperatorNode.Physical, bindFunction, out)
                return node.output?.copyWithOutput(p) ?: p
            }
        }
        return null
    }

    fun recursive(function: Function<*>) {

    }
}