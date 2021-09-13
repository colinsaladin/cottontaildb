package org.vitrivr.cottontail.database.statistics.columns

import com.google.common.primitives.Shorts.max
import com.google.common.primitives.Shorts.min
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.model.values.types.Value
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class ShortValueStatistics : ValueStatistics<ShortValue>(Type.Short) {

    /**
     * Xodus serializer for [ShortValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream): ShortValueStatistics {
            val stat = ShortValueStatistics()
            stat.min = ShortBinding.BINDING.readObject(stream)
            stat.max = ShortBinding.BINDING.readObject(stream)
            return stat
        }

        fun write(output: LightOutputStream, statistics: ShortValueStatistics) {
            ShortBinding.BINDING.writeObject(output, statistics.min)
            ShortBinding.BINDING.writeObject(output, statistics.max)
        }
    }

    /** Minimum value for this [ShortValueStatistics]. */
    var min: Short = Short.MAX_VALUE

    /** Minimum value for this [ShortValueStatistics]. */
    var max: Short = Short.MIN_VALUE

    /**
     * Updates this [LongValueStatistics] with an inserted [IntValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: ShortValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
        }
    }

    /**
     * Updates this [LongValueStatistics] with a deleted [IntValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: ShortValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.min == deleted?.value || this.max == deleted?.value) {
            this.fresh = false
        }
    }

    /**
     * Resets this [ShortValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = Short.MAX_VALUE
        this.max = Short.MIN_VALUE
    }

    /**
     * Copies this [ShortValueStatistics] and returns it.
     *
     * @return Copy of this [ShortValueStatistics].
     */
    override fun copy(): ShortValueStatistics {
        val copy = ShortValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        return copy
    }
}