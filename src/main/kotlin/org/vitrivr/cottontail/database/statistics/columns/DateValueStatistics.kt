package org.vitrivr.cottontail.database.statistics.columns

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DateValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.types.Value
import java.io.ByteArrayInputStream
import java.lang.Long.max
import java.lang.Long.min

/**
 * A [ValueStatistics] implementation for [DateValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DateValueStatistics : ValueStatistics<DateValue>(Type.Date) {

    /**
     * Xodus serializer for [DateValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream): DateValueStatistics {
            val stat = DateValueStatistics()
            stat.min = LongBinding.readCompressed(stream)
            stat.max = LongBinding.readCompressed(stream)
            return stat
        }

        fun write(output: LightOutputStream, statistics: DateValueStatistics) {
            LongBinding.writeCompressed(output, statistics.min)
            LongBinding.writeCompressed(output, statistics.max)
        }
    }

    /** Minimum value for this [DateValueStatistics]. */
    var min: Long = Long.MAX_VALUE

    /** Minimum value for this [DateValueStatistics]. */
    var max: Long = Long.MIN_VALUE

    /**
     * Updates this [LongValueStatistics] with an inserted [LongValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: DateValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.min = min(inserted.value, this.min)
            this.max = max(inserted.value, this.max)
        }
    }

    /**
     * Updates this [LongValueStatistics] with a deleted [LongValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: DateValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.min == deleted?.value || this.max == deleted?.value) {
            this.fresh = false
        }
    }

    /**
     * Resets this [DateValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = Long.MAX_VALUE
        this.max = Long.MIN_VALUE
    }

    /**
     * Copies this [DateValueStatistics] and returns it.
     *
     * @return Copy of this [DateValueStatistics].
     */
    override fun copy(): DateValueStatistics {
        val copy = DateValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        return copy
    }
}