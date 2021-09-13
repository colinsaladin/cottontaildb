package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.LongVectorValue
import org.vitrivr.cottontail.model.values.StringValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LongVectorValueXodusBinding(val size: Int): XodusBinding<LongVectorValue>() {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override fun readObject(stream: ByteArrayInputStream) = LongVectorValue(LongArray(this.size) {
        LongBinding.BINDING.readObject(stream)
    })

    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is LongVectorValue) { "Cannot serialize value of type $`object` to LongVectorValue." }
        require(`object`.logicalSize == this.size) { "Dimension ${`object`.logicalSize} of $`object` does not match size ${this.size} of this binding." }
        for (d in `object`.data) {
            LongBinding.BINDING.writeObject(output, d)
        }
    }
}