package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DoubleValueXodusBinding: XodusBinding<DoubleValue>() {
    override fun readObject(stream: ByteArrayInputStream) = DoubleValue(DoubleBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is DoubleValue) { "Cannot serialize value of type $`object` to DoubleValue." }
        DoubleBinding.BINDING.writeObject(output, `object`.value)
    }
}