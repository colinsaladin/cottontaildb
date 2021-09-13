package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.IntVectorValue
import org.vitrivr.cottontail.model.values.StringValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [IntVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IntVectorValueXodusBinding(val size: Int): XodusBinding<IntVectorValue>() {
    init {
        require(this.size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override fun readObject(stream: ByteArrayInputStream) = IntVectorValue(IntArray(this.size) {
        IntegerBinding.BINDING.readObject(stream)
    })

    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is IntVectorValue) { "Cannot serialize value of type $`object` to IntVectorValue." }
        require(`object`.logicalSize == this.size) { "Dimension ${`object`.logicalSize} of $`object` does not match size ${this.size} of this binding." }
        for (d in `object`.data) {
            IntegerBinding.BINDING.writeObject(output, d)
        }
    }
}