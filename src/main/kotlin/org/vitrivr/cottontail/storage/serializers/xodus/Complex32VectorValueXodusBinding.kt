package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.model.values.Complex64Value
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [Complex32VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32VectorValueXodusBinding(val size: Int): XodusBinding<Complex32VectorValue> {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<Complex32VectorValue>
        get() = TODO("Not yet implemented")

    override fun entryToValue(entry: ByteIterable): Complex32VectorValue {
        TODO("Not yet implemented")
    }

    override fun valueToEntry(value: Complex32VectorValue): ByteIterable {
        TODO("Not yet implemented")
    }
}