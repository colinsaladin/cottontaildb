package org.vitrivr.cottontail.database.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.statistics.selectivity.Selectivity
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value
import java.io.ByteArrayInputStream
import java.lang.Math.floorDiv

/**
 * A basic implementation of a [ValueStatistics] object, which is used by Cottontail DB to collect and summary statistics about
 * [Value]s it encounters.
 *
 * These classes are used to collect statistics about columns, which can then be leveraged by the query planner.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
open class ValueStatistics<T : Value>(val type: Type<T>) {

    /** Flag indicating that this [ValueStatistics] needs updating. */
    var fresh: Boolean = true
        protected set

    /** Number of null entries known to this [ValueStatistics]. */
    var numberOfNullEntries: Long = 0L
        protected set

    /** Number of non-null entries known to this [ValueStatistics]. */
    var numberOfNonNullEntries: Long = 0L
        protected set

    companion object Binding {
        fun read(stream: ByteArrayInputStream, type: Type<*>): ValueStatistics<*> {
            val stat = when (type) {
                Type.Complex32,
                Type.Complex64,
                is Type.Complex32Vector,
                is Type.Complex64Vector -> ValueStatistics(type)
                Type.Boolean -> BooleanValueStatistics.Binding.read(stream)
                Type.Byte -> ByteValueStatistics.Binding.read(stream)
                Type.Double -> DoubleValueStatistics.Binding.read(stream)
                Type.Float -> FloatValueStatistics.Binding.read(stream)
                Type.Int -> IntValueStatistics.Binding.read(stream)
                Type.Long -> LongValueStatistics.Binding.read(stream)
                Type.Short -> ShortValueStatistics.Binding.read(stream)
                Type.String -> StringValueStatistics.Binding.read(stream)
                Type.Date -> DateValueStatistics.Binding.read(stream)
                is Type.BooleanVector -> BooleanVectorValueStatistics.Binding.read(stream, type)
                is Type.DoubleVector -> DoubleVectorValueStatistics.Binding.read(stream, type)
                is Type.FloatVector -> FloatVectorValueStatistics.Binding.read(stream, type)
                is Type.IntVector -> IntVectorValueStatistics.Binding.read(stream, type)
                is Type.LongVector -> LongVectorValueStatistics.Binding.read(stream, type)
            }
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        fun write(output: LightOutputStream, statistics: ValueStatistics<*>) {
            when (statistics) {
                is BooleanValueStatistics -> BooleanValueStatistics.Binding.write(output, statistics)
                is ByteValueStatistics -> ByteValueStatistics.Binding.write(output, statistics)
                is ShortValueStatistics -> ShortValueStatistics.Binding.write(output, statistics)
                is IntValueStatistics -> IntValueStatistics.Binding.write(output, statistics)
                is LongValueStatistics -> LongValueStatistics.Binding.write(output, statistics)
                is FloatValueStatistics -> FloatValueStatistics.Binding.write(output, statistics)
                is DoubleValueStatistics -> DoubleValueStatistics.Binding.write(output, statistics)
                is DateValueStatistics -> DateValueStatistics.Binding.write(output, statistics)
                is StringValueStatistics -> StringValueStatistics.Binding.write(output, statistics)
                is BooleanVectorValueStatistics -> BooleanVectorValueStatistics.Binding.write(output, statistics)
                is DoubleVectorValueStatistics -> DoubleVectorValueStatistics.Binding.write(output, statistics)
                is FloatVectorValueStatistics -> FloatVectorValueStatistics.Binding.write(output, statistics)
                is LongVectorValueStatistics -> LongVectorValueStatistics.Binding.write(output, statistics)
                is IntVectorValueStatistics -> IntVectorValueStatistics.Binding.write(output, statistics)
            }
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }

    /** Total number of entries known to this [ValueStatistics]. */
    val numberOfEntries
        get() = this.numberOfNullEntries + this.numberOfNonNullEntries

    /** Smallest [Value] seen in terms of space requirement (logical size) known to this [ValueStatistics]. */
    open val minWidth: Int
        get() = this.type.logicalSize

    /** Largest [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    open val maxWidth: Int
        get() = this.type.logicalSize

    /** Mean [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    val avgWidth: Int
        get() = floorDiv(this.minWidth + this.maxWidth, 2)

    /**
     * Updates this [ValueStatistics] with an inserted [Value]
     *
     * @param inserted The [Value] that was deleted.
     */
    open fun insert(inserted: T?) {
        if (inserted == null) {
            this.numberOfNullEntries += 1
        } else {
            this.numberOfNonNullEntries += 1
        }
    }

    /**
     * Updates this [ValueStatistics] with a deleted [Value]
     *
     * @param deleted The [Value] that was deleted.
     */
    open fun delete(deleted: T?) {
        if (deleted == null) {
            this.numberOfNullEntries -= 1
        } else {
            this.numberOfNonNullEntries -= 1
        }
    }

    /**
     * Updates this [ValueStatistics] with a new updated value [T].
     *
     * Default implementation is simply a combination of [insert] and [delete].
     *
     * @param old The [Value] before the update.
     * @param new The [Value] after the update.
     */
    open fun update(old: T?, new: T?) {
        this.delete(old)
        this.insert(new)
    }

    /**
     * Resets this [ValueStatistics] and sets all its values to to the default value.
     */
    open fun reset() {
        this.fresh = true
        this.numberOfNullEntries = 0L
        this.numberOfNonNullEntries = 0L
    }

    /**
     * Copies this [ValueStatistics] and returns it.
     *
     * @return Copy of this [ValueStatistics].
     */
    open fun copy(): ValueStatistics<T> {
        val copy = ValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        return copy
    }

    /**
     * Estimates [Selectivity] of the given [BooleanPredicate.Atomic], i.e., the percentage of [Record]s that match it.
     * Defaults to [Selectivity.DEFAULT_SELECTIVITY] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Atomic] To estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    open fun estimateSelectivity(predicate: BooleanPredicate.Atomic): Selectivity = Selectivity.DEFAULT_SELECTIVITY
}