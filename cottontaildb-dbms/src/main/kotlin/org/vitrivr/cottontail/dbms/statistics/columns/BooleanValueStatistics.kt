package org.vitrivr.cottontail.dbms.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [ValueStatistics] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class BooleanValueStatistics : ValueStatistics<BooleanValue>(Types.Boolean) {

    /**
     * Serializer for [LongValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<BooleanValueStatistics> {
        override fun serialize(out: DataOutput2, value: BooleanValueStatistics) {
            out.packLong(value.numberOfTrueEntries)
            out.packLong(value.numberOfFalseEntries)
        }

        override fun deserialize(input: DataInput2, available: Int): BooleanValueStatistics {
            val stat = BooleanValueStatistics()
            stat.numberOfTrueEntries = input.unpackLong()
            stat.numberOfFalseEntries = input.unpackLong()
            return stat
        }
    }

    /** Number of true entries for in this [BooleanValueStatistics]. */
    var numberOfTrueEntries: Long = 0L
        private set

    /** Number of false entries for in this [BooleanValueStatistics]. */
    var numberOfFalseEntries: Long = 0L
        private set

    /**
     * Updates this [LongValueStatistics] with an inserted [BooleanValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: BooleanValue?) {
        when (inserted?.value) {
            null -> this.numberOfNullEntries += 1
            true -> {
                this.numberOfTrueEntries += 1
                this.numberOfNonNullEntries += 1
            }
            false -> {
                this.numberOfFalseEntries += 1
                this.numberOfNonNullEntries += 1
            }
        }
    }

    /**
     * Updates this [LongValueStatistics] with a deleted [BooleanValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: BooleanValue?) {
        when (deleted?.value) {
            null -> this.numberOfNullEntries -= 1
            true -> {
                this.numberOfTrueEntries -= 1
                this.numberOfNonNullEntries -= 1
            }
            false -> {
                this.numberOfFalseEntries -= 1
                this.numberOfNonNullEntries -= 1
            }
        }
    }

    /**
     * Resets this [BooleanValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.numberOfTrueEntries = 0L
        this.numberOfFalseEntries = 0L
    }

    /**
     * Copies this [BooleanValueStatistics] and returns it.
     *
     * @return Copy of this [BooleanValueStatistics].
     */
    override fun copy(): BooleanValueStatistics {
        val copy = BooleanValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.numberOfTrueEntries = this.numberOfTrueEntries
        copy.numberOfTrueEntries = this.numberOfTrueEntries
        return copy
    }
}
