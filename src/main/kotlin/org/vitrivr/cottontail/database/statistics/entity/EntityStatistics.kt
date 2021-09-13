package org.vitrivr.cottontail.database.statistics.entity

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.model.basics.TupleId
import kotlin.math.max

/**
 * A data object that collects statistics for an entity.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntityStatistics(var count: Long = 0L, var maximumTupleId: TupleId = -1) : RecordStatistics() {

    /**
     * Serializer for [EntityStatistics] object.
     */
    companion object Serializer : org.mapdb.Serializer<EntityStatistics> {
        override fun serialize(out: DataOutput2, value: EntityStatistics) {
            out.packLong(value.count)
            out.packLong(value.maximumTupleId)
        }

        override fun deserialize(input: DataInput2, available: Int): EntityStatistics = EntityStatistics(input.unpackLong(), input.unpackLong())
    }

    /**
     * Creates an exact copy of this [EntityStatistics].
     *
     * @return Copy of this [EntityStatistics].
     */
    override fun copy(): EntityStatistics {
        val copy = EntityStatistics(this.count, this.maximumTupleId)
        for ((t, u) in this.columns) {
            copy[t] = u.copy()
        }
        return copy
    }
}