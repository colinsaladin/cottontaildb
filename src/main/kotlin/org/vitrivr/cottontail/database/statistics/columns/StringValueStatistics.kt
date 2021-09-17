package org.vitrivr.cottontail.database.statistics.columns

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value
import java.io.ByteArrayInputStream
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A specialized [ValueStatistics] for [StringValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class StringValueStatistics : ValueStatistics<StringValue>(Type.String) {

    /**
     * Xodus serializer for [ShortValueStatistics]
     */
    object Binding {
        fun read(stream: ByteArrayInputStream): StringValueStatistics {
            val stat = StringValueStatistics()
            stat.minWidth = IntegerBinding.readCompressed(stream)
            stat.maxWidth = IntegerBinding.readCompressed(stream)
            return stat
        }

        fun write(output: LightOutputStream, statistics: StringValueStatistics) {
            IntegerBinding.writeCompressed(output, statistics.minWidth)
            IntegerBinding.writeCompressed(output, statistics.maxWidth)
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