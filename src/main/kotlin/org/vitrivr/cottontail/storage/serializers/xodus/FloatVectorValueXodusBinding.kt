package org.vitrivr.cottontail.storage.serializers.xodus

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.ByteIterableUtil
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.FloatVectorValue
import java.io.ByteArrayInputStream

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

    override fun entryToValue(entry: ByteIterable): FloatVectorValue {
        val stream = ByteArrayInputStream(entry.bytesUnsafe)
        repeat(this.size) { this.array[it] = FloatBinding.BINDING.readObject(stream) }
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