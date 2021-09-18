package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [Complex64VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64VectorValueXodusBinding(val size: Int): XodusBinding<Complex64VectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<Complex64VectorValue>
        get() = TODO("Not yet implemented")

    override fun entryToValue(entry: ByteIterable): Complex64VectorValue {
        TODO("Not yet implemented")
    }

    override fun valueToEntry(value: Complex64VectorValue): ByteIterable {
        TODO("Not yet implemented")
    }
}