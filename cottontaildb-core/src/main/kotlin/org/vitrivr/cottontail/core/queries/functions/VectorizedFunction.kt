package org.vitrivr.cottontail.core.queries.functions

import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A binary, vectorized [Function] used for distance calculations between a query [VectorValue] and other [VectorValue]s.
 *
 * @author Colin Saladin
 * @version 1.0.0
 */
interface VectorizedFunction<R: Value> : Function<R> {

    /** The dimensionality of this [VectorizedFunction]. */
    val d: Int

    /** Signature of a [VectorizedFunction] is defined by the argument type it accepts. */
    override val signature: Signature.Closed<R>

}