package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A specialized [ValueStatistics] for [StringValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class StringValueStatistics : ValueStatistics<StringValue>(Types.String) {

    /**
     * Serializer for [StringValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<StringValueStatistics> {
        override fun serialize(out: DataOutput2, value: StringValueStatistics) {
            out.packInt(value.minWidth)
            out.packInt(value.maxWidth)
        }

        override fun deserialize(input: DataInput2, available: Int): StringValueStatistics {
            val stat = StringValueStatistics()
            stat.minWidth = input.unpackInt()
            stat.maxWidth = input.unpackInt()
            return stat
        }
    }

    /** Smallest [StringValue] seen by this [ValueStatistics] */
    override var minWidth: Int = Int.MAX_VALUE
        private set

    /** Largest [StringValue] seen by this [ValueStatistics] */
    override var maxWidth: Int = Int.MIN_VALUE
        private set

    /**
     * Updates this [StringValueStatistics] with an inserted [StringValue].
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: StringValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.minWidth = min(inserted.logicalSize, this.minWidth)
            this.maxWidth = max(inserted.logicalSize, this.maxWidth)
        }
    }

    /**
     * Updates this [StringValueStatistics] with a new deleted [StringValue].
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: StringValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (deleted != null) {
            if (this.minWidth == deleted.logicalSize || this.maxWidth == deleted.logicalSize) {
                this.fresh = false
            }
        }
    }

    /**
     * Resets this [StringValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.minWidth = Int.MAX_VALUE
        this.maxWidth = Int.MIN_VALUE
    }

    /**
     * Copies this [StringValueStatistics] and returns it.
     *
     * @return Copy of this [StringValueStatistics].
     */
    override fun copy(): StringValueStatistics {
        val copy = StringValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.minWidth = this.minWidth
        copy.maxWidth = this.maxWidth
        return copy
    }
}