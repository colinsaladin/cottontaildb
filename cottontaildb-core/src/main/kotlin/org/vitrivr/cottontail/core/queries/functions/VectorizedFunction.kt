package org.vitrivr.cottontail.core.queries.functions

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import kotlin.Function

/**
 * A binary, vectorizable [Function] used for distance calculations between a query [VectorValue] and other [VectorValue]s.
 *
 * This type of [Function] always accepts two [VectorValue] of the same [Types.Vector] (query argument and probing argument)
 * and always returns a [Types.Double].
 *
 * @author Colin Saladin
 * @version 1.0.0
 */
interface VectorizedFunction<T: VectorValue<*>> : Function<VectorValue<*>> {

    /** The [Types.Vector] accepted by this [VectorDistance]. */
    val name: Name.FunctionName

    /** The dimensionality of this [VectorDistance]. */
    val d: Int

    /** Signature of a [VectorDistance] is defined by the argument type it accepts. */
    val signature: Signature.Closed<out VectorValue<*>>

    /**
     * Creates a copy of this [VectorizableFunction].
     *
     * @return Copy of this [VectorizableFunction]
     */
    fun copy(): VectorizableFunction<T> = copy(this.d)

    /**
     * Creates a reshaped copy of this [VectorizableFunction].
     *
     * @return Copy of this [VectorizableFunction]
     */
    fun copy(d: Int): VectorizableFunction<T>

    /**
     * Returns the vectorized Version of the [VectorizableFunction].
     *
     * @return Vectorized [VectorizableFunction]
     */
    fun vectorized(): VectorizableFunction<T>
}