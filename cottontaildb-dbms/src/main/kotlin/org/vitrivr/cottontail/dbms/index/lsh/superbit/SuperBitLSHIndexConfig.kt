package org.vitrivr.cottontail.dbms.index.lsh.superbit

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.dbms.index.IndexConfig

/**
 * Configuration object for [SuperBitLSHIndex].
 *
 * @author Gabriel Zihlmann
 * @version 1.1.0
 */
data class SuperBitLSHIndexConfig(
    val buckets: Int,
    val stages: Int,
    val seed: Long,
    val considerImaginary: Boolean,
    val samplingMethod: SamplingMethod
) : IndexConfig {
    companion object Serializer : org.mapdb.Serializer<SuperBitLSHIndexConfig> {

        const val SEED = "seed"
        const val NUM_STAGES = "stages"
        const val NUM_BUCKETS = "buckets"
        const val CONSIDER_IMAGINARY = "considerimaginary"
        const val SAMPLING_METHOD = "samplingmethod"

        /**
         *
         */
        override fun serialize(out: DataOutput2, value: SuperBitLSHIndexConfig) {
            out.packInt(value.buckets)
            out.packInt(value.stages)
            out.packLong(value.seed)
            out.packInt(if (value.considerImaginary) 1 else 0)
            out.packInt(value.samplingMethod.ordinal)
        }

        /**
         *
         */
        override fun deserialize(input: DataInput2, available: Int) = SuperBitLSHIndexConfig(
            input.unpackInt(),
            input.unpackInt(),
            input.unpackLong(),
            input.unpackInt() != 0,
            SamplingMethod.values()[input.unpackInt()]
        )

        /**
         * Constructs a [SuperBitLSHIndexConfig] from a parameter map.
         *
         * @param params The parameter map.
         * @return SuperBitLSHIndexConfig
         */
        fun fromParamMap(params: Map<String, String>) = SuperBitLSHIndexConfig(
            buckets = params[NUM_BUCKETS]!!.toInt(),
            stages = params[NUM_STAGES]!!.toInt(),
            seed = params[SEED]!!.toLong(),
            considerImaginary = params[CONSIDER_IMAGINARY]!!.toBoolean(),
            samplingMethod = SamplingMethod.valueOf(params[SAMPLING_METHOD]!!)
        )
    }

    /**
     * Converts this [SuperBitLSHIndexConfig] to a [Map] representation.
     *
     * @return [Map] representation of this [SuperBitLSHIndexConfig].
     */
    override fun toMap(): Map<String, String> = mapOf(
        NUM_BUCKETS to this.buckets.toString(),
        NUM_STAGES to this.stages.toString(),
        SEED to this.seed.toString(),
        CONSIDER_IMAGINARY to considerImaginary.toString(),
        SAMPLING_METHOD to this.samplingMethod.toString()
    )
}

