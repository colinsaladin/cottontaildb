package org.vitrivr.cottontail.core.queries.functions

import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A binary, vectorizable [Function] used for distance calculations between a query [VectorValue] and other [VectorValue]s.
 *
 * @author Colin Saladin
 * @version 1.0.0
 */
interface VectorizableFunction<R: Value> : Function<R> {

    /** The dimensionality of this [VectorizableFunction]. */
    val d: Int

    /** Signature of a [VectorizableFunction] is defined by the argument type it accepts. */
    override val signature: Signature.Closed<R>

    /**
     * Creates a copy of this [VectorizableFunction].
     *
     * @return Copy of this [VectorizableFunction]
     */
    override fun copy(): VectorizableFunction<R> = copy(this.d)

    /**
     * Creates a reshaped copy of this [VectorizableFunction].
     *
     * @return Copy of this [VectorizableFunction]
     */
    fun copy(d: Int): VectorizableFunction<R>

    /**
     * Returns the vectorized Version of the [VectorizableFunction].
     *
     * @return The [VectorizedFunction] of this [VectorizableFunction]
     */
    fun vectorized(): VectorizedFunction<R>
}