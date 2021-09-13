package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DoubleVectorValueXodusBinding(val size: Int): XodusBinding<DoubleVectorValue>() {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override fun readObject(stream: ByteArrayInputStream) = DoubleVectorValue(DoubleArray(this.size) {
        DoubleBinding.BINDING.readObject(stream)
    })

    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is DoubleVectorValue) { "Cannot serialize value of type $`object` to DoubleVectorValue." }
        require(`object`.logicalSize == this.size) { "Dimension ${`object`.logicalSize} of $`object` does not match size ${this.size} of this binding." }
        for (d in `object`.data) {
            DoubleBinding.BINDING.writeObject(output, d)
        }
    }
}