package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.ByteValue
import org.vitrivr.cottontail.model.values.Complex64Value
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [ByteValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ByteValueXodusBinding: XodusBinding<ByteValue>() {
    override fun readObject(stream: ByteArrayInputStream) = ByteValue(ByteBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is ByteValue) { "Cannot serialize value of type $`object` to ByteValue." }
        ByteBinding.BINDING.writeObject(output, `object`.value)
    }
}