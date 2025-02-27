package org.vitrivr.cottontail.dbms.queries.operators.logical.projection

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.queries.operators.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.ExistsProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [UnaryLogicalOperatorNode] that represents a projection operation involving aggregate functions such as [Projection.EXISTS].
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class ExistsProjectionLogicalOperatorNode(input: Logical? = null, val out: Binding.Column) : AbstractProjectionLogicalOperatorOperator(input, Projection.EXISTS) {

    /** The [ColumnDef] generated by this [ExistsProjectionLogicalOperatorNode]. */
    override val columns: List<ColumnDef<*>>
        get() = listOf(this.out.column)

    /**
     * Creates and returns a copy of this [ExistsProjectionLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [ExistsProjectionLogicalOperatorNode].
     */
    override fun copy() = ExistsProjectionLogicalOperatorNode(out = this.out)

    /**
     * Returns a [ExistsProjectionPhysicalOperatorNode] representation of this [ExistsProjectionLogicalOperatorNode]
     *
     * @return [ExistsProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = ExistsProjectionPhysicalOperatorNode(this.input?.implement(), this.out)

    /**
     * Compares this [ExistsProjectionPhysicalOperatorNode] to another object.
     *
     * @param other The other [Any] to compare this [ExistsProjectionPhysicalOperatorNode] to.
     * @return True if other equals this, false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ExistsProjectionLogicalOperatorNode) return false
        if (this.type != other.type) return false
        if (this.out != other.out) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [ExistsProjectionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.type.hashCode()
        result = 31 * result + this.out.hashCode()
        return result
    }
}