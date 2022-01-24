package org.vitrivr.cottontail.database.statistics.columns

import com.google.common.primitives.Shorts.max
import com.google.common.primitives.Shorts.min
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [ValueStatistics] implementation for [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class ShortValueStatistics : ValueStatistics<ShortValue>(Types.Short) {

    /**
     * Serializer for [LongValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<ShortValueStatistics> {
        override fun serialize(out: DataOutput2, value: ShortValueStatistics) {
            out.writeShort(value.min.toInt())
            out.writeShort(value.max.toInt())
        }

        override fun deserialize(input: DataInput2, available: Int): ShortValueStatistics {
            val stat = ShortValueStatistics()
            stat.min = input.readShort()
            stat.max = input.readShort()
            return stat
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