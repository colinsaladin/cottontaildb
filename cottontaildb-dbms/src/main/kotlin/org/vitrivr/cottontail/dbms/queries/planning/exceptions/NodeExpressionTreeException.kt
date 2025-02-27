package org.vitrivr.cottontail.dbms.queries.planning.exceptions

import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode

/**
 * Type of [Exception]s that are thrown while processing a [OperatorNode] or a [OperatorNode] tree.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
sealed class NodeExpressionTreeException(val exp: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, message: String) :
    Exception(message) {
    /**
     * Thrown when a [OperatorNode] tree is incomplete, i.e. [OperatorNode]s are missing, while trying process it.
     *
     * @param exp The [OperatorNode] that caused the exception.
     * @param message Explanation of the problem.
     */
    class IncompleteNodeExpressionTreeException(exp: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, message: String) :
        NodeExpressionTreeException(exp, "NodeExpression $exp seems incomplete: $message")

    /**
     * Thrown when a [OperatorNode] cannot be materialized due to constraints of incoming or outgoing [OperatorNode]s.
     *
     * @param exp The [OperatorNode] that caused the exception.
     * @param message Explanation of the problem.
     */
    class InsatisfiableNodeExpressionException(exp: org.vitrivr.cottontail.dbms.queries.operators.OperatorNode, message: String) :
        NodeExpressionTreeException(exp, "NodeExpression $exp seems incomplete: $message")
}

