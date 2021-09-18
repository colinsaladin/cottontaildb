package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.IntValue

/**
 * A [XodusBinding] for [IntValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object IntValueXodusBinding: XodusBinding<IntValue> {
    override val type = Type.Int
    override fun entryToValue(entry: ByteIterable): IntValue = IntValue(IntegerBinding.BINDING.entryToObject(entry) as Int)
    override fun valueToEntry(value: IntValue): ByteIterable = IntegerBinding.BINDING.objectToEntry(value.value)
}