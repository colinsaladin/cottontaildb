package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.model.values.Complex64Value
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [Complex32VectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32VectorValueXodusBinding(val size: Int): XodusBinding<Complex32VectorValue>() {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override fun readObject(stream: ByteArrayInputStream) = Complex32VectorValue(FloatArray(2 * this.size) {
        FloatBinding.BINDING.readObject(stream)
    })

    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is Complex32VectorValue) { "Cannot serialize value of type $`object` to Complex32VectorValue." }
        require(`object`.logicalSize == this.size) { "Dimension ${`object`.logicalSize} of $`object` does not match size ${this.size} of this binding." }
        for (d in `object`.data) {
            FloatBinding.BINDING.writeObject(output, d)
        }
    }
}