package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Policy

/**
 * Config for Cottontail DB's cost sub-system.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
class CostConfig {
    /** The weight of the IO portion of a [Cost] towards the overall score. */
    val iow: Float = 0.6f

    /** The weight of the CPU portion of a [Cost] towards the overall score. */
    val cpuw: Float = 0.2f

    /** The weight of the  Memory portion of a [Cost] towards the overall score. */
    val memw: Float = 0.2f

    /** The weight of the accuracy portion of a [Cost] towards the overall score. */
    val accw: Float= 0.0f

    /**
     * Returns a [Policy] object for this [CostConfig].
     *
     * @return [Policy]
     */
    fun policy(): Policy = Policy(this.iow, this.cpuw, this.memw, this.accw)
}