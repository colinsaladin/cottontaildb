package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.ByteValue

/**
 * A [NumericValueGenerator] for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ByteValueGenerator: NumericValueGenerator<ByteValue> {
    override fun random(rnd: RandomGenerator) = ByteValue(rnd.nextInt(Byte.MAX_VALUE.toInt()).toByte())
    override fun one() = ByteValue.ONE
    override fun zero() = ByteValue.ZERO
    override fun of(number: Number) = ByteValue(number)
}