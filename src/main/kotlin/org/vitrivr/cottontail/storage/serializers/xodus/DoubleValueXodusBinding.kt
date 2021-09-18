package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.DoubleBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleValue

/**
 * A [XodusBinding] for [DoubleValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DoubleValueXodusBinding: XodusBinding<DoubleValue> {
    override val type = Type.Double
    override fun entryToValue(entry: ByteIterable): DoubleValue = DoubleValue(DoubleBinding.BINDING.entryToObject(entry) as Double)
    override fun valueToEntry(value: DoubleValue): ByteIterable = DoubleBinding.BINDING.objectToEntry(value.value)
}