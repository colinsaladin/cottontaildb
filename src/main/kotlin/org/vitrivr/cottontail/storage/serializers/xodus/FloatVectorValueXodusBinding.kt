package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.FloatVectorValue
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [XodusBinding] for [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class FloatVectorValueXodusBinding(size: Int): XodusBinding<FloatVectorValue> {

    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<FloatVectorValue> = Type.FloatVector(size)

    /**
     * [FloatVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): FloatVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): FloatVectorValue {
            val stream = ByteArrayInputStream(entry.bytesUnsafe)
            return FloatVectorValue(FloatArray(this.type.logicalSize) { FloatBinding.BINDING.readObject(stream) })
        }

        override fun valueToEntry(value: FloatVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val stream = LightOutputStream(this.type.physicalSize)
            repeat(this.type.logicalSize) { FloatBinding.BINDING.writeObject(stream, value.data[it]) }
            return stream.asArrayByteIterable()
        }
    }

    /**
     * [FloatVectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): FloatVectorValueXodusBinding(size) {
        companion object {
            private val NULL_VALUE = FloatBinding.BINDING.objectToEntry(Float.MIN_VALUE)
        }

        override fun entryToValue(entry: ByteIterable): FloatVectorValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                val stream = ByteArrayInputStream(bytesRead)
                return FloatVectorValue(FloatArray(this.type.logicalSize) { FloatBinding.BINDING.readObject(stream) })
            }
        }

        override fun valueToEntry(value: FloatVectorValue?): ByteIterable = if (value == null) {
            NULL_VALUE
        } else {
            val stream = LightOutputStream(this.type.physicalSize)
            repeat(this.type.logicalSize) { FloatBinding.BINDING.writeObject(stream, value.data[it]) }
            stream.asArrayByteIterable()
        }
    }
}