package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.StringValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [FloatVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FloatVectorValueXodusBinding(val size: Int): XodusBinding<FloatVectorValue>() {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override fun readObject(stream: ByteArrayInputStream) = FloatVectorValue(FloatArray(this.size) {
        FloatBinding.BINDING.readObject(stream)
    })

    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is FloatVectorValue) { "Cannot serialize value of type $`object` to FloatVectorValue." }
        require(`object`.logicalSize == this.size) { "Dimension ${`object`.logicalSize} of $`object` does not match size ${this.size} of this binding." }
        for (d in `object`.data) {
            FloatBinding.BINDING.writeObject(output, d)
        }
    }
}