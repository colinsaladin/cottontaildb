package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.StringValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [StringBinding] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object StringValueXodusBinding: XodusBinding<StringValue>() {
    override fun readObject(stream: ByteArrayInputStream) = StringValue(StringBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is StringValue) { "Cannot serialize value of type $`object` to StringValue." }
        StringBinding.BINDING.writeObject(output, `object`.value)
    }
}