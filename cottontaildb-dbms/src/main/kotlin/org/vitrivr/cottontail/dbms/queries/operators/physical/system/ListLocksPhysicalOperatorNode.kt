package org.vitrivr.cottontail.dbms.queries.operators.physical.system

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.operators.system.ListLocksOperator
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.queries.operators.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics

/**
 * A [NullaryPhysicalOperatorNode] used to list all locks.
 *
 * @author Ralph Gasser
 * @version 1.0.0.
 */
class ListLocksPhysicalOperatorNode(val manager: LockManager<DBO>): NullaryPhysicalOperatorNode() {
    override val groupId: GroupId = 0
    override val name: String = "ListLocks"
    override val outputSize: Long = 1L
    override val statistics: Map<ColumnDef<*>, ValueStatistics<*>>
        get() = emptyMap()
    override val columns: List<ColumnDef<*>>
        get() = ColumnSets.DDL_LOCKS_COLUMNS
    override val physicalColumns: List<ColumnDef<*>>
        get() = emptyList()
    override val cost: Cost = Cost.ZERO
    override fun toOperator(ctx: QueryContext) = ListLocksOperator(this.manager)
    override fun copy() = ListLocksPhysicalOperatorNode(this.manager)
}