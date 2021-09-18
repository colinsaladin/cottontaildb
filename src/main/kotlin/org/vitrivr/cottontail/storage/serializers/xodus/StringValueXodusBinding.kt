package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.StringValue

/**
 * A [ComparableBinding] for Xodus based [StringBinding] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object StringValueXodusBinding: XodusBinding<StringValue> {
    override val type = Type.String
    override fun entryToValue(entry: ByteIterable): StringValue = StringValue(StringBinding.BINDING.entryToObject(entry) as String)
    override fun valueToEntry(value: StringValue): ByteIterable = StringBinding.BINDING.objectToEntry(value.value)
}