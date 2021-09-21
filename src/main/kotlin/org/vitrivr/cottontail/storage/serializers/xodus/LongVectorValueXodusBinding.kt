package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.LongVectorValue
import org.vitrivr.cottontail.model.values.ShortValue
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util.*

/**
 * A [XodusBinding] for [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class LongVectorValueXodusBinding(val size: Int): XodusBinding<LongVectorValue> {

    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<LongVectorValue> = Type.LongVector(this.size)

    /**
     * [LongVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): LongVectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): LongVectorValue {
            val bytesRead = entry.bytesUnsafe
            val stream = ByteArrayInputStream(bytesRead)
            return LongVectorValue(LongArray(this.size) { LongBinding.BINDING.readObject(stream) })
        }

        override fun valueToEntry(value: LongVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val stream = LightOutputStream(this.type.physicalSize)
            repeat(this.size) { LongBinding.BINDING.writeObject(stream, value.data[it]) }
            return stream.asArrayByteIterable()
        }
    }

    /**
     * [LongVectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): LongVectorValueXodusBinding(size) {

        companion object {
            private val NULL_VALUE = LongBinding.BINDING.objectToEntry(Long.MIN_VALUE)
        }

        override fun entryToValue(entry: ByteIterable): LongVectorValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                val stream = ByteArrayInputStream(bytesRead)
                return LongVectorValue(LongArray(this.size) { LongBinding.BINDING.readObject(stream) })
            }
        }

        override fun valueToEntry(value: LongVectorValue?): ByteIterable = if (value == null) {
            NULL_VALUE
        } else {
            val stream = LightOutputStream(this.type.physicalSize)
            repeat(this.size) { LongBinding.BINDING.writeObject(stream, value.data[it]) }
            stream.asArrayByteIterable()
        }
    }

}