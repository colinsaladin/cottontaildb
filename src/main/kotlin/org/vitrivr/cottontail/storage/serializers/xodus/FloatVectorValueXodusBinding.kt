package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BindingUtils
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.FloatVectorValue
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

/**
 * A [XodusBinding] for [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorValueXodusBinding(val size: Int): XodusBinding<FloatVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<FloatVectorValue> = Type.FloatVector(this.size)
    private val array = FloatArray(this.size)
    private val buffer = ByteBuffer.allocate(8)
    override fun entryToValue(entry: ByteIterable): FloatVectorValue {
        val iterator = entry.bytesUnsafe
        var i = 0
        while (iterator.hasNext()) {
            this.buffer.clear()
            this.buffer.putLong(iterator.nextLong(8))
            this.buffer.flip()
            this.array[i++] = buffer.float
            this.array[i++] = buffer.float
        }
        return FloatVectorValue(this.array)
    }

    override fun valueToEntry(value: FloatVectorValue): ByteIterable {
        val output = LightOutputStream(this.type.physicalSize)
        for (v in value.data) {
            FloatBinding.BINDING.writeObject(output, v)
        }
        return output.asArrayByteIterable()
    }
}