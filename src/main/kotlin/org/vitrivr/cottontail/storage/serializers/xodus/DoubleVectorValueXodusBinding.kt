package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [XodusBinding] for [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class DoubleVectorValueXodusBinding(size: Int): XodusBinding<DoubleVectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<DoubleVectorValue> = Type.DoubleVector(size)

    /**
     * [DoubleVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): DoubleVectorValueXodusBinding(size) {
        companion object {
            private val NULL_VALUE = DoubleBinding.BINDING.objectToEntry(Double.MIN_VALUE)
        }

        override fun entryToValue(entry: ByteIterable): DoubleVectorValue {
            val stream = ByteArrayInputStream(entry.bytesUnsafe)
            return DoubleVectorValue(DoubleArray(this.type.logicalSize) { DoubleBinding.BINDING.readObject(stream) })
        }

        override fun valueToEntry(value: DoubleVectorValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            val stream = LightOutputStream(this.type.physicalSize)
            repeat(this.type.logicalSize) { DoubleBinding.BINDING.writeObject(stream, value.data[it]) }
            return stream.asArrayByteIterable()
        }
    }

    /**
     * [DoubleVectorValueXodusBinding] used for non-nullable values.
     */
    class Nullable(size: Int): DoubleVectorValueXodusBinding(size) {
        companion object {
            private val NULL_VALUE = DoubleBinding.BINDING.objectToEntry(Double.MIN_VALUE)
        }

        override fun entryToValue(entry: ByteIterable): DoubleVectorValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                val stream = ByteArrayInputStream(bytesRead)
                return DoubleVectorValue(DoubleArray(this.type.logicalSize) { DoubleBinding.BINDING.readObject(stream) })
            }
        }

        override fun valueToEntry(value: DoubleVectorValue?): ByteIterable = if (value == null) {
            NULL_VALUE
        } else {
            val stream = LightOutputStream(this.type.physicalSize)
            repeat(this.type.logicalSize) { DoubleBinding.BINDING.writeObject(stream, value.data[it]) }
            stream.asArrayByteIterable()
        }
    }
}