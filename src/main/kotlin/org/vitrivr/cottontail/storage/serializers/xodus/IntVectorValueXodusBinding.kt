package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.IntVectorValue
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [XodusBinding] for [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class IntVectorValueXodusBinding(val size: Int): XodusBinding<IntVectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<IntVectorValue> = Type.IntVector(this.size)

    /**
     * [IntVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): IntVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): IntVectorValue {
            val stream = ByteArrayInputStream(entry.bytesUnsafe)
            return IntVectorValue(IntArray(this.size) { IntegerBinding.BINDING.readObject(stream) })
        }

        override fun valueToEntry(value: IntVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val stream = LightOutputStream(this.type.physicalSize)
            repeat(this.size) { IntegerBinding.BINDING.writeObject(stream, value.data[it]) }
            return stream.asArrayByteIterable()
        }
    }

    /**
     * [IntVectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): IntVectorValueXodusBinding(size) {
        companion object {
            private val NULL_VALUE = IntegerBinding.BINDING.objectToEntry(Int.MIN_VALUE)
        }

        override fun entryToValue(entry: ByteIterable): IntVectorValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                val stream = ByteArrayInputStream(bytesRead)
                return IntVectorValue(IntArray(this.size) { IntegerBinding.BINDING.readObject(stream) })
            }
        }

        override fun valueToEntry(value: IntVectorValue?): ByteIterable = if (value == null) {
            NULL_VALUE
        } else {
            val stream = LightOutputStream(this.type.physicalSize)
            repeat(this.size) { IntegerBinding.BINDING.writeObject(stream, value.data[it]) }
            stream.asArrayByteIterable()
        }
    }
}