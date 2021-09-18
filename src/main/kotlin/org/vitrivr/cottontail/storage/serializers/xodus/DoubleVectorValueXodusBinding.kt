package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import java.nio.ByteBuffer

/**
 * A [XodusBinding] for [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleVectorValueXodusBinding(val size: Int): XodusBinding<DoubleVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<DoubleVectorValue> = Type.DoubleVector(this.size)

    private val buffer = ByteBuffer.allocate(this.type.physicalSize)

    private val array = DoubleArray(this.size)

    override fun entryToValue(entry: ByteIterable): DoubleVectorValue {
        this.buffer.clear()
        this.buffer.put(entry.bytesUnsafe)
        this.buffer.flip()
        repeat(this.size) { this.array[it] = this.buffer.double }
        return DoubleVectorValue(this.array)
    }

    override fun valueToEntry(value: DoubleVectorValue): ByteIterable {
        this.buffer.clear()
        for (v in value.data) this.buffer.putDouble(v)
        return LightOutputStream(this.buffer.array()).asArrayByteIterable()
    }
}