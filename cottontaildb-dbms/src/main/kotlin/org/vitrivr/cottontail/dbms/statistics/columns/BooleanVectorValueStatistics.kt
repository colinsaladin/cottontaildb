package org.vitrivr.cottontail.dbms.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A [ValueStatistics] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanVectorValueStatistics(type: Types<BooleanVectorValue>) : ValueStatistics<BooleanVectorValue>(type) {

    /** A histogram capturing the number of true entries per component. */
    val numberOfTrueEntries: LongArray = LongArray(this.type.logicalSize)

    /** A histogram capturing the number of false entries per component. */
    val numberOfFalseEntries: LongArray
        get() = LongArray(this.type.logicalSize) {
            this.numberOfNonNullEntries - this.numberOfTrueEntries[it]
        }

    /**
     * Updates this [DoubleVectorValueStatistics] with an inserted [DoubleVectorValue]
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: BooleanVectorValue?) {
        super.insert(inserted)
        if (inserted != null) {
            for ((i, d) in inserted.data.withIndex()) {
                if (d) this.numberOfTrueEntries[i] = this.numberOfTrueEntries[i] + 1
            }
        }
    }

    /**
     * Updates this [DoubleVectorValueStatistics] with a deleted [DoubleVectorValue]
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: BooleanVectorValue?) {
        super.delete(deleted)
        if (deleted != null) {
            for ((i, d) in deleted.data.withIndex()) {
                if (d) this.numberOfTrueEntries[i] = this.numberOfTrueEntries[i] - 1
            }
        }
    }

    /**
     * A [org.mapdb.Serializer] implementation for a [DoubleVectorValueStatistics] object.
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    class Serializer(val type: Types<BooleanVectorValue>) : org.mapdb.Serializer<BooleanVectorValueStatistics> {
        override fun serialize(out: DataOutput2, value: BooleanVectorValueStatistics) {
            value.numberOfTrueEntries.forEach { out.writeLong(it) }
        }

        override fun deserialize(input: DataInput2, available: Int): BooleanVectorValueStatistics {
            val stat = BooleanVectorValueStatistics(this.type)
            repeat(this.type.logicalSize) { stat.numberOfTrueEntries[it] = input.readLong() }
            return stat
        }
    }

    /**
     * Resets this [BooleanVectorValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.numberOfTrueEntries[i] = 0L
        }
    }

    /**
     * Copies this [BooleanVectorValueStatistics] and returns it.
     *
     * @return Copy of this [BooleanVectorValueStatistics].
     */
    override fun copy(): BooleanVectorValueStatistics {
        val copy = BooleanVectorValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        for (i in 0 until this.type.logicalSize) {
            copy.numberOfTrueEntries[i] = this.numberOfTrueEntries[i]
        }
        return copy
    }
}