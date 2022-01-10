package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.projection.SelectDistinctProjectionOperator
import org.vitrivr.cottontail.execution.operators.projection.SelectProjectionOperator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * Formalizes a [UnaryPhysicalOperatorNode] operation in the Cottontail DB query execution engine.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class SelectProjectionPhysicalOperatorNode(input: Physical? = null, val type: Projection, val fields: List<Name.ColumnName>): UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Select"
    }

    init {
        /* Sanity check. */
        require(this.type in arrayOf(Projection.SELECT, Projection.SELECT_DISTINCT)) {
            "Projection of type ${this.type} cannot be used with instances of AggregatingProjectionLogicalNodeExpression."
        }
    }

    /** The name of this [SelectProjectionPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [ColumnDef] generated by this [SelectProjectionPhysicalOperatorNode]. */
    override val columns: List<ColumnDef<*>>
        get() = this.input?.columns?.filter { c -> fields.any { f -> f == c.name }} ?: emptyList()

    /** The [ColumnDef] required by this [SelectProjectionPhysicalOperatorNode]. */
    override val requires: List<ColumnDef<*>>
        get() = this.columns

    /** The [Cost] of a [SelectProjectionPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.outputSize * this.fields.size * Cost.COST_MEMORY_ACCESS)

    init {
        /* Sanity check. */
        if (this.fields.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
        }
    }

    /**
     * Creates and returns a copy of this [SelectProjectionPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [SelectProjectionPhysicalOperatorNode].
     */
    override fun copy() = SelectProjectionPhysicalOperatorNode(type = this.type, fields = this.fields)

    /**
     * Partitions this [SelectProjectionPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> =
        this.input?.partition(p)?.map { SelectProjectionPhysicalOperatorNode(it, this.type, this.fields) } ?: throw IllegalStateException("Cannot partition disconnected OperatorNode (node = $this)")

    /**
     * Converts this [SelectProjectionPhysicalOperatorNode] to a [SelectProjectionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return when (this.type) {
            Projection.SELECT -> SelectProjectionOperator(input.toOperator(ctx), this.fields)
            Projection.SELECT_DISTINCT -> SelectDistinctProjectionOperator(input.toOperator(ctx), this.fields, input.outputSize)
            else -> throw IllegalArgumentException("SelectProjectionPhysicalOperatorNode can only have type SELECT or SELECT_DISTINCT. This is a programmer's error!")
        }
    }

    /** Generates and returns a [String] representation of this [SelectProjectionPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SelectProjectionPhysicalOperatorNode) return false
        if (this.type != other.type) return false
        if (this.fields != other.fields) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [SelectProjectionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.type.hashCode()
        result = 31 * result + this.fields.hashCode()
        return result
    }
}