package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.Complex32VectorValue

/**
 * A [ComparableBinding] for Xodus based [Complex32VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Complex32VectorValueXodusBinding(size: Int): XodusBinding<Complex32VectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<Complex32VectorValue> = Type.Complex32Vector(size)

    /**
     * [Complex32VectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): Complex32VectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): Complex32VectorValue? {
            TODO("Not yet implemented")
        }

        override fun valueToEntry(value: Complex32VectorValue?): ByteIterable {
            TODO("Not yet implemented")
        }

    }

    /**
     * [Complex32VectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): Complex32VectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): Complex32VectorValue? {
            TODO("Not yet implemented")
        }

        override fun valueToEntry(value: Complex32VectorValue?): ByteIterable {
            TODO("Not yet implemented")
        }
    }
}