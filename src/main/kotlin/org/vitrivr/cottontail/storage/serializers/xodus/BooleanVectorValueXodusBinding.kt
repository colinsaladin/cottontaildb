package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.BooleanVectorValue

/**
 * A [ComparableBinding] for Xodus based [BooleanVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanVectorValueXodusBinding(val size: Int): XodusBinding<BooleanVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<BooleanVectorValue>
        get() = TODO("Not yet implemented")

    override fun entryToValue(entry: ByteIterable): BooleanVectorValue {
        TODO("Not yet implemented")
    }

    override fun valueToEntry(value: BooleanVectorValue): ByteIterable {
        TODO("Not yet implemented")
    }
}