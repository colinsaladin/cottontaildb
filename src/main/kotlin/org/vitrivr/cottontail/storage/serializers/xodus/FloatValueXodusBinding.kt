package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.FloatValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FloatValueXodusBinding: XodusBinding<FloatValue>() {
    override fun readObject(stream: ByteArrayInputStream) = FloatValue(FloatBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is FloatValue) { "Cannot serialize value of type $`object` to FloatValue." }
        FloatBinding.BINDING.writeObject(output, `object`.value)
    }
}