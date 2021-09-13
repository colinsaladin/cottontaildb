package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.BooleanVectorValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [BooleanVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BooleanVectorValueXodusBinding(val size: Int): XodusBinding<BooleanVectorValue>() {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override fun readObject(stream: ByteArrayInputStream) = BooleanVectorValue(BooleanArray(this.size) {
        BooleanBinding.BINDING.readObject(stream)
    })

    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is BooleanVectorValue) { "Cannot serialize value of type $`object` to BooleanVectorValue." }
        require(`object`.logicalSize == this.size) { "Dimension ${`object`.logicalSize} of $`object` does not match size ${this.size} of this binding." }
        for (d in `object`.data) {
            BooleanBinding.BINDING.writeObject(output, d)
        }
    }
}