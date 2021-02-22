package org.vitrivr.cottontail.database.queries.planning.rules.physical.pushdown

import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.ProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityCountPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.database.queries.projection.Projection

/**
 * Pushes the simple counting of entries in an [Entity] down.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object CountPushdownRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean =
        node is ProjectionPhysicalOperatorNode && node.type == Projection.COUNT

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is ProjectionPhysicalOperatorNode && node.type == Projection.COUNT) {
            val input = node.input
            if (input is EntityScanPhysicalOperatorNode) {
                val p = EntityCountPhysicalOperatorNode(input.entity)
                node.copyOutput()?.addInput(p)
                return p
            }
        }
        return null
    }
}