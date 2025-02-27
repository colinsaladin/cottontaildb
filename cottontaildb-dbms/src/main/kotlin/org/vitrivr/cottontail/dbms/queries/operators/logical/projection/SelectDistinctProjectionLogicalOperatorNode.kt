package org.vitrivr.cottontail.dbms.queries.operators.logical.projection

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.SelectDistinctProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [AbstractProjectionLogicalOperatorOperator] that formalizes a [Projection.SELECT_DISTINCT] operation in a Cottontail DB query plan.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SelectDistinctProjectionLogicalOperatorNode(input: Logical? = null, val fields: List<Pair<Name.ColumnName, Boolean>>, val config: Config): AbstractProjectionLogicalOperatorOperator(input, Projection.SELECT_DISTINCT) {
    /** The name of this [SelectDistinctProjectionLogicalOperatorNode]. */
    override val name: String = "SelectDistinct"

    /** The [ColumnDef] generated by this [SelectProjectionLogicalOperatorNode]. */
    override val columns: List<ColumnDef<*>>
        get() = this.input?.columns?.filter { c -> this.fields.any { f -> f.first == c.name }} ?: emptyList()

    /** The [ColumnDef] required by this [SelectProjectionLogicalOperatorNode]. */
    override val requires: List<ColumnDef<*>>
        get() = this.columns

    init {
        /* Sanity check. */
        if (this.fields.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
        }
    }

    /**
     * Creates and returns a copy of this [SelectDistinctProjectionLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [SelectDistinctProjectionLogicalOperatorNode].
     */
    override fun copy() = SelectDistinctProjectionLogicalOperatorNode(fields = this.fields, config = this.config)

    /**
     * Returns a [SelectDistinctProjectionPhysicalOperatorNode] representation of this [SelectDistinctProjectionPhysicalOperatorNode]
     *
     * @return [SelectDistinctProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = SelectDistinctProjectionPhysicalOperatorNode(this.input?.implement(), this.fields, this.config)

    /**
     * Compares this [SelectDistinctProjectionLogicalOperatorNode] to another object.
     *
     * @param other The other [Any] to compare this [SelectDistinctProjectionLogicalOperatorNode] to.
     * @return True if other equals this, false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SelectDistinctProjectionLogicalOperatorNode) return false
        if (this.type != other.type) return false
        if (this.fields != other.fields) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [SelectProjectionLogicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.type.hashCode()
        result = 33 * result + this.fields.hashCode()
        return result
    }
}