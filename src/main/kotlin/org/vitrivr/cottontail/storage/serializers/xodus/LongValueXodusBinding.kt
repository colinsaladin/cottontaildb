package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.LongValue
/**
 * A [XodusBinding] for [LongValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object LongValueXodusBinding: XodusBinding<LongValue> {
    override val type = Type.Long
    override fun entryToValue(entry: ByteIterable): LongValue = LongValue(LongBinding.BINDING.entryToObject(entry) as Long)
    override fun valueToEntry(value: LongValue): ByteIterable = LongBinding.BINDING.objectToEntry(value.value)
}