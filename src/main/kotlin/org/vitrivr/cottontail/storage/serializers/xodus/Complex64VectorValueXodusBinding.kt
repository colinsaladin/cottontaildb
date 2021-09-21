package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.Complex64VectorValue

/**
 * A [ComparableBinding] for Xodus based [Complex64VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Complex64VectorValueXodusBinding(size: Int): XodusBinding<Complex64VectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<Complex64VectorValue> = Type.Complex64Vector(size)

    /**
     * [Complex64VectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): Complex64VectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): Complex64VectorValue? {
            TODO("Not yet implemented")
        }

        override fun valueToEntry(value: Complex64VectorValue?): ByteIterable {
            TODO("Not yet implemented")
        }
    }

    /**
     * [Complex64VectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): Complex64VectorValueXodusBinding(size) {
        override fun entryToValue(entry: ByteIterable): Complex64VectorValue? {
            TODO("Not yet implemented")
        }

        override fun valueToEntry(value: Complex64VectorValue?): ByteIterable {
            TODO("Not yet implemented")
        }
    }
}