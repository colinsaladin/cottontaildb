package org.vitrivr.cottontail.database.queries.planning.rules.physical.index

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.RewriteRule
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [RewriteRule] that implements a [FilterLogicalOperatorNode] preceded by a
 * [EntityScanLogicalOperatorNode] through a single [IndexScanPhysicalOperatorNode].
 *
 * @author Ralph Gasser
 * @version 1.2.1
 */
object BooleanIndexScanRule : RewriteRule {
    override fun canBeApplied(node: OperatorNode): Boolean =
        node is FilterPhysicalOperatorNode && node.input is EntityScanPhysicalOperatorNode

    override fun apply(node: OperatorNode, ctx: QueryContext): OperatorNode? {
        if (node is FilterPhysicalOperatorNode) {
            val parent = node.input
            if (parent is EntityScanPhysicalOperatorNode) {
                val fetch = parent.fetch.toMap()
                val normalizedPredicate = this.normalize(node.predicate, fetch)
                val indexes = parent.entity.listIndexes()
                val candidate = indexes.map {
                    parent.entity.indexForName(it)
                }.find {
                    it.canProcess(normalizedPredicate)
                }
                if (candidate != null) {
                    val newFetch = parent.fetch.filter { candidate.produces.contains(it.second) }
                    val delta = parent.fetch.filter { !candidate.produces.contains(it.second) }
                    var p: OperatorNode.Physical = IndexScanPhysicalOperatorNode(node.groupId, ctx.txn.getTx(candidate) as IndexTx, node.predicate, newFetch)
                    if (delta.isNotEmpty()) {
                        p = FetchPhysicalOperatorNode(p, parent.entity, delta)
                    }
                    return node.output?.copyWithOutput(p) ?: p
                }
            }
        }
        return null
    }

    /**
     * Normalizes the given [BooleanPredicate] given the list of mapped [ColumnDef]s. Normalization resolves
     * potential [ColumnDef] containing alias names to the root [ColumnDef]s.
     *
     * @param predicate [BooleanPredicate] To normalize.
     * @param fetch [Map] of [ColumnDef] and alias [Name.ColumnName].
     */
    private fun normalize(predicate: BooleanPredicate, fetch: Map<Name.ColumnName,ColumnDef<*>>): BooleanPredicate = when (predicate) {
        is BooleanPredicate.Atomic -> {
            /* Map left and right operands. */
            val op = predicate.operator
            val left = if (op.left is Binding.Column) {
                Binding.Column(fetch[op.left.column.name]!!, op.left.context)
            } else {
                op.left
            }
            val right: List<Binding> = when(op) {
                is ComparisonOperator.Binary -> {
                    listOf(if (op.right is Binding.Column) {
                        Binding.Column(fetch[op.right.column.name]!!, op.right.context)
                    } else {
                        op.right
                    })
                }
                is ComparisonOperator.Between -> {
                    listOf(
                        if (op.rightLower is Binding.Column) {
                            Binding.Column(fetch[op.rightLower.column.name]!!, op.rightLower.context)
                        } else {
                            op.rightLower
                        },
                        if (op.rightUpper is Binding.Column) {
                            Binding.Column(fetch[op.rightUpper.column.name]!!, op.rightUpper.context)
                        } else {
                            op.rightUpper
                        }
                    )
                }
                else -> emptyList<Binding>()
            }

            /* Return new operator. */
            val newOp = when(op) {
                is ComparisonOperator.Between -> ComparisonOperator.Between(left, right[0], right[1])
                is ComparisonOperator.Binary.Equal -> ComparisonOperator.Binary.Equal(left, right[0])
                is ComparisonOperator.Binary.Greater -> ComparisonOperator.Binary.Greater(left, right[0])
                is ComparisonOperator.Binary.GreaterEqual -> ComparisonOperator.Binary.GreaterEqual(left, right[0])
                is ComparisonOperator.Binary.Less -> ComparisonOperator.Binary.Less(left, right[0])
                is ComparisonOperator.Binary.LessEqual -> ComparisonOperator.Binary.LessEqual(left, right[0])
                is ComparisonOperator.Binary.Like -> ComparisonOperator.Binary.Like(left, right[0])
                is ComparisonOperator.Binary.Match -> ComparisonOperator.Binary.Match(left, right[0])
                is ComparisonOperator.In -> op /* IN operators only support literal bindings. */
                is ComparisonOperator.IsNull -> ComparisonOperator.IsNull(left)
            }
            BooleanPredicate.Atomic(newOp, predicate.not, predicate.dependsOn)
        }
        is BooleanPredicate.Compound -> BooleanPredicate.Compound(predicate.connector, normalize(predicate.p1, fetch), normalize(predicate.p2, fetch))
    }
}