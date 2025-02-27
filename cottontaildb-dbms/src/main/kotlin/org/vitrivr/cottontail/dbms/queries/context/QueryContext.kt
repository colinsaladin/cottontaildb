package org.vitrivr.cottontail.dbms.queries.context

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.QueryHint
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner

/**
 * A context for query binding, planning and execution. The [QueryContext] bundles all the
 * relevant aspects of a query such as  the logical and physical plans, [TransactionContext]
 * and [BindingContext]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface QueryContext {

    /** An identifier for this [QueryContext]. */
    val queryId: String

    /** The [Catalogue] this [QueryContext] belongs to.. */
    val catalogue: Catalogue

    /** The [Transaction] the query held by this [QueryContext] is associated with. */
    val txn: Transaction

    /** The [BindingContext] exposed by this [QueryContext]. */
    val bindings: BindingContext

    /** Set of [QueryHint]s held by this [QueryContext]. These hints influence query planning. */
    val hints: Set<QueryHint>

    /** The [CostPolicy] that should be applied within this [QueryContext] */
    val costPolicy: CostPolicy

    /** The [OperatorNode.Logical] representing the query and the sub-queries held by this [QueryContext]. */
    val logical: OperatorNode.Logical?

    /** The [OperatorNode.Physical] representing the query and the sub-queries held by this [QueryContext]. */
    val physical: OperatorNode.Physical?

    /** Output [ColumnDef] for the query held by this [QueryContext] (as per canonical plan). */
    val output: List<ColumnDef<*>>?

    /** Output order for the query held by this [QueryContext] (as per canonical plan). */
    val order: List<Pair<ColumnDef<*>, SortOrder>>

    /**
     * Returns the next available [GroupId].
     *
     * @return Next available [GroupId].
     */
    fun nextGroupId(): GroupId

    /**
     * Assigns a new [OperatorNode.Logical] to this [QueryContext] overwriting the existing [OperatorNode.Logical].
     *
     * Invalidates all existing [OperatorNode.Logical] and [OperatorNode.Physical] held by this [QueryContext].
     *
     * @param plan The [OperatorNode.Logical] to assign.
     */
    fun assign(plan: OperatorNode.Logical)

    /**
     * Assigns a new [OperatorNode.Physical] to this [QueryContext] overwriting the existing [OperatorNode.Physical].
     *
     * Invalidates all existing [OperatorNode.Logical] and [OperatorNode.Physical] held by this [QueryContext].
     *
     * @param plan The [OperatorNode.Physical] to assign.
     */
    fun assign(plan: OperatorNode.Physical)

    /**
     * Starts the query planning processing using the given [CottontailQueryPlanner]. The query planning
     * process tries to generate a near-optimal [OperatorNode.Physical] from the registered [OperatorNode.Logical].
     *
     * @param planner The [CottontailQueryPlanner] instance to use for planning.
     */
    fun plan(planner: CottontailQueryPlanner)

    /**
     * Converts the registered [OperatorNode.Logical] to the equivalent [OperatorNode.Physical] and skips query planning.
     */
    fun implement()

    /**
     * Splits this [QueryContext] into a subcontext.
     *
     * A subcontext usually shares the majority of properties with the parent [QueryContext],
     * but uses its dedicated [BindingContext]. This is mainly used to allow for parallelisation.
     */
    fun split(): QueryContext

    /**
     * Implements the query held by this [QueryContext]. Requires a functional, [OperatorNode.Physical]
     *
     * @return [Operator]
     */
    fun toOperatorTree(): Operator
}