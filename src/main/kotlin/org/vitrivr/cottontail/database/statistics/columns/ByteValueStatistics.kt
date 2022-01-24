package org.vitrivr.cottontail.database.statistics.columns

import com.google.common.primitives.SignedBytes.max
import com.google.common.primitives.SignedBytes.min
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.ByteValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [ValueStatistics] implementation for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ByteValueStatistics : ValueStatistics<ByteValue>(Types.Byte) {

    /**
     * Serializer for [LongValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<ByteValueStatistics> {
        override fun serialize(out: DataOutput2, value: ByteValueStatistics) {
            out.writeByte(value.min.toInt())
            out.writeByte(value.max.toInt())
        }

        override fun deserialize(input: DataInput2, available: Int): ByteValueStatistics {
            val stat = ByteValueStatistics()
            stat.min = input.readByte()
            stat.max = input.readByte()
            return stat
        }
    }

    /** Minimum value for this [ByteValueStatistics]. */
    var min: Byte = Byte.MAX_VALUE

    /** Minimum value for this [ByteValueStatistics]. */
    var max: Byte = Byte.MIN_VALUE

    /**
     * Updates this [LongValueStatistics] with an inserted [IntValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: ByteValue?) {
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
    override fun delete(deleted: ByteValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (deleted != null) {
            if (this.min == deleted.value || this.max == deleted.value) {
                this.fresh = false
            }
        }
    }

    /**
     * Resets this [ByteValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.min = Byte.MAX_VALUE
        this.max = Byte.MIN_VALUE
    }

    /**
     * Copies this [ByteValueStatistics] and returns it.
     *
     * @return Copy of this [ByteValueStatistics].
     */
    override fun copy(): ByteValueStatistics {
        val copy = ByteValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.min = this.min
        copy.max = this.max
        return copy
    }
}