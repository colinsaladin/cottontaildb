package org.vitrivr.cottontail.dbms.queries.planning.rules.physical.simd

import org.vitrivr.cottontail.core.queries.functions.VectorizableFunction
import org.vitrivr.cottontail.core.queries.functions.VectorizedFunction
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

    override fun canBeApplied(node: OperatorNode, ctx: QueryContext): Boolean = node is FunctionPhysicalOperatorNode &&
            node.function.function is VectorizableFunction<*>

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {

        if (node is FunctionPhysicalOperatorNode && node.function.function is VectorizableFunction<*> && node.function.function !is VectorizedFunction<*>) {
            val input = node.input?.copy() ?: return null
            val out = node.out

            // Provisional heuristic
            if ((node.function.function as VectorizableFunction<*>).d >= 256) {
                val bindFunction = out.context.bind((node.function.function as VectorizableFunction<*>).vectorized(), node.function.arguments)
                val p = FunctionPhysicalOperatorNode(input as OperatorNode.Physical, bindFunction, out)
                return node.output?.copyWithOutput(p) ?: p
            }
        }
        return null
    }

}