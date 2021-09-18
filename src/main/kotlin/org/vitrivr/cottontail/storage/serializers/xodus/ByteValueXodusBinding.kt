package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ByteBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.ByteValue

/**
 * A [XodusBinding] for [ByteValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ByteValueXodusBinding: XodusBinding<ByteValue> {
    override val type = Type.Byte
    override fun entryToValue(entry: ByteIterable): ByteValue = ByteValue(ByteBinding.BINDING.entryToObject(entry) as Byte)
    override fun valueToEntry(value: ByteValue): ByteIterable = ByteBinding.BINDING.objectToEntry(value.value)
}