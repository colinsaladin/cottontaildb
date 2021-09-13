package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.model.values.StringValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object LongValueXodusBinding: XodusBinding<LongValue>(){
    override fun readObject(stream: ByteArrayInputStream) = LongValue(LongBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is LongValue) { "Cannot serialize value of type $`object` to LongValue." }
        LongBinding.BINDING.writeObject(output, `object`.value)
    }
}