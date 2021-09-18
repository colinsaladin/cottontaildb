package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BooleanBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.BooleanValue

/**
 * A [XodusBinding] for [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object BooleanValueXodusBinding: XodusBinding<BooleanValue>{
    override val type = Type.Boolean
    override fun entryToValue(entry: ByteIterable): BooleanValue = BooleanValue(BooleanBinding.BINDING.entryToObject(entry) as Boolean)
    override fun valueToEntry(value: BooleanValue): ByteIterable = BooleanBinding.BINDING.objectToEntry(value.value)
}