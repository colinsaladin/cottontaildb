package org.vitrivr.cottontail.core.queries.functions

import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A binary, vectorized [Function] used for distance calculations between a query [VectorValue] and other [VectorValue]s.
 * Nearly
 *
 * This type of [Function] always accepts two [VectorValue] of the same [Types.Vector] (query argument and probing argument)
 * and always returns a [Types.Double].
 *
 * @author Colin Saladin
 * @version 1.0.0
 */
interface VectorizedFunction<R: Value> : Function<R> {

    /** The dimensionality of this [VectorDistance]. */
    val d: Int

    /** Signature of a [VectorDistance] is defined by the argument type it accepts. */
    override val signature: Signature.Closed<R>

}