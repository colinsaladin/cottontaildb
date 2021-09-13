package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.model.values.BooleanVectorValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object BooleanValueXodusBinding: XodusBinding<BooleanValue>(){
    override fun readObject(stream: ByteArrayInputStream) = BooleanValue(BooleanBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is BooleanValue) { "Cannot serialize value of type $`object` to BooleanValue." }
        BooleanBinding.BINDING.writeObject(output, `object`.value)
    }
}