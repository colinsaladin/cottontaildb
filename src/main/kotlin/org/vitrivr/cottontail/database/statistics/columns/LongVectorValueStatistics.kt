package org.vitrivr.cottontail.database.statistics.columns

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.IntVectorValue
import org.vitrivr.cottontail.model.values.LongVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import java.io.ByteArrayInputStream

import java.lang.Long.max
import java.lang.Long.min

/**
 * A [ValueStatistics] implementation for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LongVectorValueStatistics(type: Type<LongVectorValue>) : ValueStatistics<LongVectorValue>(type) {
    /** Minimum value in this [LongVectorValueStatistics]. */
    val min: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize) { Long.MAX_VALUE })

    /** Minimum value in this [LongVectorValueStatistics]. */
    val max: LongVectorValue = LongVectorValue(LongArray(this.type.logicalSize) { Long.MIN_VALUE })

    /**
     * Xodus serializer for [LongVectorValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream, type: Type<LongVectorValue>): LongVectorValueStatistics {
            val stat = LongVectorValueStatistics(type)
            for (i in 0 until type.logicalSize) {
                stat.min.data[i] = LongBinding.BINDING.readObject(stream)
                stat.max.data[i] = LongBinding.BINDING.readObject(stream)
            }
            return stat
        }

        fun write(output: LightOutputStream, statistics: LongVectorValueStatistics) {
            for (i in 0 until statistics.type.logicalSize) {
                LongBinding.BINDING.writeObject(output, statistics.min.data[i])
                LongBinding.BINDING.writeObject(output, statistics.max.data[i])
            }
        }
    }

    /**
     * Updates this [LongVectorValueStatistics] with an inserted [LongVectorValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: LongVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                this.min.data[i] = min(d, this.min.data[i])
                this.max.data[i] = max(d, this.max.data[i])
            }
        }
    }

    /**
     * Updates this [LongVectorValueStatistics] with a deleted [LongVectorValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: LongVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                if (this.min.data[i] == d || this.max.data[i] == d) {
                    this.fresh = false
                }
            }
        }
    }

    /**
     * Resets this [LongVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.min.data[i] = Long.MAX_VALUE
            this.max.data[i] = Long.MIN_VALUE
        }
    }

    /**
     * Copies this [LongVectorValueStatistics] and returns it.
     *
     * @return Copy of this [LongVectorValueStatistics].
     */
    override fun copy(): LongVectorValueStatistics {
        val copy = LongVectorValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        for (i in 0 until this.type.logicalSize) {
            copy.min.data[i] = this.min.data[i]
            copy.max.data[i] = this.max.data[i]
        }
        return copy
    }
}