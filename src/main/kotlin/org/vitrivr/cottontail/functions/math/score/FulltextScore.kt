package org.vitrivr.cottontail.functions.math.score

import org.vitrivr.cottontail.functions.basics.Argument
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * This function is a mere placeholder used to express fulltext searches.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
object FulltextScore: Function<DoubleValue> {
    override val signature: Signature.Closed<out DoubleValue> = Signature.Closed(Name.FunctionName("fulltext"), arrayOf(Argument.Typed(Types.String), Argument.Typed(Types.String)), Types.Double)
    override val executable: Boolean = false
    override val cost: Float = Float.POSITIVE_INFINITY
    override fun invoke(): DoubleValue = throw UnsupportedOperationException("Function ${this.signature} cannot be executed directly.")
    override fun provide(index: Int, value: Value?) = throw UnsupportedOperationException("Function ${this.signature} cannot be executed directly.")
}