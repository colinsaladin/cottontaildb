package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.DateValue
import org.vitrivr.cottontail.model.values.DoubleValue
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DateValueXodusBinding: XodusBinding<DateValue>() {
    override fun readObject(stream: ByteArrayInputStream) = DateValue(LongBinding.readCompressed(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is DateValue) { "Cannot serialize value of type $`object` to DateValue." }
        LongBinding.writeCompressed(output, `object`.value)
    }
}