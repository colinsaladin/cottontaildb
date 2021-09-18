package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.LongVectorValue
import java.nio.ByteBuffer

/**
 * A [XodusBinding] for [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongVectorValueXodusBinding(val size: Int): XodusBinding<LongVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<LongVectorValue> = Type.LongVector(this.size)
    private val buffer = ByteBuffer.allocate(this.type.physicalSize)
    override fun entryToValue(entry: ByteIterable): LongVectorValue {
        this.buffer.clear()
        this.buffer.put(entry.bytesUnsafe)
        this.buffer.flip()
        return LongVectorValue(LongArray(this.size) { this.buffer.long })
    }

    override fun valueToEntry(value: LongVectorValue): ByteIterable {
        this.buffer.clear()
        for (v in value.data) this.buffer.putLong(v)
        return LightOutputStream(this.buffer.array()).asArrayByteIterable()
    }
}