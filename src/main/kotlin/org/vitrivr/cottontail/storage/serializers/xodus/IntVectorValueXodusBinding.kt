package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.IntVectorValue
import java.nio.ByteBuffer

/**
 * A [XodusBinding] for [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntVectorValueXodusBinding(val size: Int): XodusBinding<IntVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<IntVectorValue> = Type.IntVector(this.size)
    private val buffer = ByteBuffer.allocate(this.type.physicalSize)
    override fun entryToValue(entry: ByteIterable): IntVectorValue {
        this.buffer.clear()
        this.buffer.put(entry.bytesUnsafe)
        this.buffer.flip()
        return IntVectorValue(IntArray(this.size) { this.buffer.int })
    }

    override fun valueToEntry(value: IntVectorValue): ByteIterable {
        this.buffer.clear()
        for (v in value.data) this.buffer.putInt(v)
        return LightOutputStream(this.buffer.array()).asArrayByteIterable()
    }
}