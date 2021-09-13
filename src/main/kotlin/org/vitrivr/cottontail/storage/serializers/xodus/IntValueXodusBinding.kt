package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.StringValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object IntValueXodusBinding: XodusBinding<IntValue>() {
    override fun readObject(stream: ByteArrayInputStream) = IntValue(IntegerBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is IntValue) { "Cannot serialize value of type $`object` to IntValue." }
        IntegerBinding.BINDING.writeObject(output, `object`.value)
    }
}