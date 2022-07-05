package org.vitrivr.cottontail.core.queries.functions.math.distance.binary

import jdk.incubator.vector.VectorOperators
import jdk.incubator.vector.VectorSpecies
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.*
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue
import kotlin.math.pow

/**
 * A [SquaredEuclideanDistance] implementation to calculate the squared Euclidean or L2 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class SquaredEuclideanDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): MinkowskiDistance<T>(type) {

    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName("squaredeuclidean")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            return when(val type = signature.arguments[0].type) {
                is Types.Complex64Vector -> Complex64Vector(type)
                is Types.Complex32Vector -> Complex32Vector(type)
                is Types.DoubleVector -> DoubleVector(type)
                is Types.FloatVector -> FloatVector(type)
                is Types.LongVector -> LongVector(type)
                is Types.IntVector -> IntVector(type)
                else -> throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            }
        }

        override fun resolve(signature: Signature.Open): List<Signature.Closed<*>> {
            if (Companion.signature != signature) throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            if (signature.arguments.any { it != signature.arguments[0] }) throw FunctionNotSupportedException("Function generator ${HaversineDistance.signature} cannot generate function with signature $signature.")
            return listOf(
                Complex64Vector(Types.Complex64Vector(1)).signature,
                Complex32Vector(Types.Complex32Vector(1)).signature,
                DoubleVector(Types.DoubleVector(1)).signature,
                FloatVector(Types.FloatVector(1)).signature,
                LongVector(Types.LongVector(1)).signature,
                IntVector(Types.IntVector(1)).signature
            )
        }
    }

    /** Signature of a [VectorDistance] is defined by the argument type it accepts. */
    override val signature: Signature.Closed<DoubleValue>
        get() = Signature.Closed(FUNCTION_NAME, arrayOf(this.type, this.type), Types.Double)

    /** The [Cost] of applying this [SquaredEuclideanDistance]. */
    override val cost: Cost
        get() = ((Cost.FLOP * 3.0f + Cost.MEMORY_ACCESS * 2.0f) * this.d) + Cost.MEMORY_ACCESS

    /** The [SquaredEuclideanDistance] is a [MinkowskiDistance] with p = 2. */
    override val p: Int
        get() = 2

    /**
     * [SquaredEuclideanDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(type: Types.Vector<Complex64VectorValue,*>): SquaredEuclideanDistance<Complex64VectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex64VectorValue
            val query = arguments[1] as Complex64VectorValue
            var sum = 0.0
            for (i in 0 until 2 * this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = Complex64Vector(Types.Complex64Vector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            TODO("@Colin Not yet implemented")
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(type: Types.Vector<Complex32VectorValue,*>): SquaredEuclideanDistance<Complex32VectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex32VectorValue
            val query = arguments[1] as Complex32VectorValue
            var sum = 0.0
            for (i in 0 until 2 * this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = Complex32Vector(Types.Complex32Vector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            TODO("@Colin Not yet implemented")
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): SquaredEuclideanDistance<DoubleVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            var sum = 0.0
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            return DoubleVectorVectorized(this.type)
        }
    }

    /**
     * SIMD implementation: [ChisquaredDistance] for a [DoubleVectorValue]
     */
    class DoubleVectorVectorized(type: Types.Vector<DoubleVectorValue,*>): ChisquaredDistance<DoubleVectorValue>(type), VectorizedFunction<DoubleValue> {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            // Changing SPECIES to SPECIES.PREFERRED results in a HUGE performance decrease
            val species: VectorSpecies<Double> = jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            var vectorSum = jdk.incubator.vector.DoubleVector.zero(species)

            //Vectorized calculation
            for (i in 0 until species.loopBound(this.d) step species.length()) {
                val vp = jdk.incubator.vector.DoubleVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.DoubleVector.fromArray(species, query.data, i)
                vectorSum = vectorSum.lanewise(VectorOperators.ADD, vp.lanewise(VectorOperators.SUB, vq)
                    .mul(vp.lanewise(VectorOperators.SUB, vq)))
            }

            var sum = vectorSum.reduceLanes(VectorOperators.ADD)

            // Scalar calculation for the remaining lanes, since SPECIES.loopBound(this.d) <= this.d
            for (i in species.loopBound(this.d) until this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }

            return DoubleValue(sum)
        }
        override fun copy(d: Int) = DoubleVectorVectorized(Types.DoubleVector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            return this
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): SquaredEuclideanDistance<FloatVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var sum = 0.0
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            return FloatVectorVectorized(this.type)
        }
    }

    /**
     * SIMD implementation: [SquaredEuclideanDistance] for a [FloatVectorValue]
     */
    class FloatVectorVectorized(type: Types.Vector<FloatVectorValue,*>): SquaredEuclideanDistance<FloatVectorValue>(type), VectorizedFunction<DoubleValue> {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            // Changing SPECIES to SPECIES.PREFERRED results in a HUGE performance decrease
            val species: VectorSpecies<Float> = jdk.incubator.vector.FloatVector.SPECIES_PREFERRED
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var vectorSum = jdk.incubator.vector.FloatVector.zero(species)

            //Vectorized calculation
            for (i in 0 until species.loopBound(this.d) step species.length()) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(species, query.data, i)
                vectorSum = vectorSum.lanewise(VectorOperators.ADD, vp.lanewise(VectorOperators.SUB, vq)
                    .mul(vp.lanewise(VectorOperators.SUB, vq)))
            }

            var sum = vectorSum.reduceLanes(VectorOperators.ADD)

            // Scalar calculation for the remaining lanes, since SPECIES.loopBound(this.d) <= this.d
            for (i in species.loopBound(this.d) until this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2f)
            }

            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVectorVectorized(Types.FloatVector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            return this
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): SquaredEuclideanDistance<LongVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            var sum = 0.0
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            return LongVectorVectorized(this.type)
        }
    }

    /**
     * SIMD implementation: [ChisquaredDistance] for a [LongVectorValue]
     */
    class LongVectorVectorized(type: Types.Vector<LongVectorValue,*>): ChisquaredDistance<LongVectorValue>(type), VectorizedFunction<DoubleValue> {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            // Changing SPECIES to SPECIES.PREFERRED results in a HUGE performance decrease
            val species: VectorSpecies<Long> = jdk.incubator.vector.LongVector.SPECIES_PREFERRED
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            var vectorSum = jdk.incubator.vector.LongVector.zero(species)

            //Vectorized calculation
            for (i in 0 until species.loopBound(this.d) step species.length()) {
                val vp = jdk.incubator.vector.LongVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.LongVector.fromArray(species, query.data, i)
                vectorSum = vectorSum.lanewise(VectorOperators.ADD, vp.lanewise(VectorOperators.SUB, vq)
                    .mul(vp.lanewise(VectorOperators.SUB, vq)))
            }

            var sum = vectorSum.reduceLanes(VectorOperators.ADD).toDouble()

            // Scalar calculation for the remaining lanes, since SPECIES.loopBound(this.d) <= this.d
            for (i in species.loopBound(this.d) until this.d) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }

            return DoubleValue(sum)
        }
        override fun copy(d: Int) = LongVectorVectorized(Types.LongVector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            return this
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): SquaredEuclideanDistance<IntVectorValue>(type) {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            var sum = 0.0
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            return IntVectorVectorized(this.type)
        }
    }

    /**
     * SIMD implementation: [ChisquaredDistance] for a [IntVectorValue]
     */
    class IntVectorVectorized(type: Types.Vector<IntVectorValue,*>): ChisquaredDistance<IntVectorValue>(type), VectorizedFunction<DoubleValue> {
        override fun invoke(vararg arguments: Value?): DoubleValue {
            // Changing SPECIES to SPECIES.PREFERRED results in a HUGE performance decrease
            val species: VectorSpecies<Int> = jdk.incubator.vector.IntVector.SPECIES_PREFERRED
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            var vectorSum = jdk.incubator.vector.IntVector.zero(species)

            //Vectorized calculation
            for (i in 0 until species.loopBound(this.d) step species.length()) {
                val vp = jdk.incubator.vector.IntVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.IntVector.fromArray(species, query.data, i)
                vectorSum = vectorSum.lanewise(VectorOperators.ADD, vp.lanewise(VectorOperators.SUB, vq)
                    .mul(vp.lanewise(VectorOperators.SUB, vq)))
            }

            var sum = vectorSum.reduceLanes(VectorOperators.ADD).toDouble()

            // Scalar calculation for the remaining lanes, since SPECIES.loopBound(this.d) <= this.d
            for (i in species.loopBound(this.d) until this.d) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }

            return DoubleValue(sum)
        }
        override fun copy(d: Int) = IntVectorVectorized(Types.IntVector(d))

        override fun vectorized(): VectorizedFunction<DoubleValue> {
            return this
        }
    }
}