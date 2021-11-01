package org.vitrivr.cottontail.database.queries.planning.cost

/**
 * A [Cost] [Policy] used to obtain a score.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@JvmInline
value class Policy private constructor(private val weights: FloatArray) {
    /**
     * Default constructor for [Policy] object.
     *
     * @param io The IO weight of the [Policy] object.
     * @param cpu The CPU weight of the [Policy] object.
     * @param memory The Memory weight of the [Policy] object.
     * @param accuracy The Accuracy weight of the [Policy] object.
     */
    constructor(io: Float = 0.0f, cpu: Float = 0.0f, memory: Float = 0.0f, accuracy: Float = 0.0f): this(floatArrayOf(io, cpu, memory, accuracy))

    /** The IO dimension of the [Cost] object. */
    val io: Float
        get() = this.weights[0]

    /** The CPU dimension of the [Cost] object. */
    val cpu: Float
        get() = this.weights[1]

    /** The Memory dimension of the [Cost] object. */
    val memory: Float
        get() = this.weights[2]

    /** The Accuracy dimension of the [Cost] object. */
    val accuracy: Float
        get() = this.weights[3]

    /**
     * Calculates and returns a [Cost] score given this [Policy].
     *
     * @param cost The [Cost] to obtain the score for
     */
    fun score(cost: Cost): Float =
        this.io * cost.io + this.cpu * cost.cpu + this.memory + cost.memory + this.accuracy + cost.accuracy
}