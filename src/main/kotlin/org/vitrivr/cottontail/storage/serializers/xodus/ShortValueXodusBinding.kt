package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.model.values.StringValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ShortValueXodusBinding: XodusBinding<ShortValue>(){
    override fun readObject(stream: ByteArrayInputStream) = ShortValue(ShortBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is StringValue) { "Cannot serialize value of type $`object` to ShortValue." }
        ShortBinding.BINDING.writeObject(output, `object`.value)
    }
}