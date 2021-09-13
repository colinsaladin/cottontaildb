package org.vitrivr.cottontail.database.statistics.columns

import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import java.io.ByteArrayInputStream
import java.lang.Float.max
import java.lang.Float.min

/**
 * A [ValueStatistics] implementation for [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FloatVectorValueStatistics(type: Type<FloatVectorValue>) : ValueStatistics<FloatVectorValue>(type) {
    /** Minimum value in this [FloatVectorValueStatistics]. */
    val min: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize) { Float.MAX_VALUE })

    /** Minimum value in this [FloatVectorValueStatistics]. */
    val max: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize) { Float.MIN_VALUE })

    /** Sum of all floats values in this [FloatVectorValueStatistics]. */
    val sum: FloatVectorValue = FloatVectorValue(FloatArray(this.type.logicalSize))

    /** The arithmetic for the values seen by this [DoubleVectorValueStatistics]. */
    val avg: FloatVectorValue
        get() = FloatVectorValue(FloatArray(this.type.logicalSize) {
            this.sum[it].value / this.numberOfNonNullEntries
        })

    /**
     * Xodus serializer for [FloatVectorValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream, type: Type<FloatVectorValue>): FloatVectorValueStatistics {
            val stat = FloatVectorValueStatistics(type)
            for (i in 0 until type.logicalSize) {
                stat.min.data[i] = FloatBinding.BINDING.readObject(stream)
                stat.max.data[i] = FloatBinding.BINDING.readObject(stream)
                stat.sum.data[i] = FloatBinding.BINDING.readObject(stream)
            }
            return stat
        }

        fun write(output: LightOutputStream, statistics: FloatVectorValueStatistics) {
            for (i in 0 until statistics.type.logicalSize) {
                FloatBinding.BINDING.writeObject(output, statistics.min.data[i])
                FloatBinding.BINDING.writeObject(output, statistics.max.data[i])
                FloatBinding.BINDING.writeObject(output, statistics.sum.data[i])
            }
        }
    }


    /**
     * Updates this [FloatVectorValueStatistics] with an inserted [FloatVectorValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: FloatVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = min(d, this.min.data[i])
                this.max.data[i] = max(d, this.max.data[i])
                this.sum.data[i] += d
            }
        }
    }

    /**
     * Updates this [FloatVectorValueStatistics] with a deleted [FloatVectorValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: FloatVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                /* We cannot create a sensible estimate if a value is deleted. */
                if (this.min.data[i] == d || this.max.data[i] == d) {
                    this.fresh = false
                }
                this.sum.data[i] -= d
            }
        }
    }

    /**
     * Resets this [FloatVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Float.MAX_VALUE
            this.max.data[i] = Float.MIN_VALUE
            this.sum.data[i] = 0.0f
        }
    }

    /**
     * Copies this [FloatVectorValueStatistics] and returns it.
     *
     * @return Copy of this [FloatVectorValueStatistics].
     */
    override fun copy(): FloatVectorValueStatistics {
        val copy = FloatVectorValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        for (i in 0 until this.type.logicalSize) {
            copy.min.data[i] = this.min.data[i]
            copy.max.data[i] = this.max.data[i]
            copy.sum.data[i] = this.sum.data[i]
        }
        return copy
    }
}