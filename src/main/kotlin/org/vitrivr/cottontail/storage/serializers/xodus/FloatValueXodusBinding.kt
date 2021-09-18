package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.FloatBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.FloatValue

/**
 * A [XodusBinding] for [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FloatValueXodusBinding: XodusBinding<FloatValue> {
    override val type = Type.Float
    override fun entryToValue(entry: ByteIterable): FloatValue = FloatValue(FloatBinding.BINDING.entryToObject(entry) as Float)
    override fun valueToEntry(value: FloatValue): ByteIterable = FloatBinding.BINDING.objectToEntry(value.value)
}