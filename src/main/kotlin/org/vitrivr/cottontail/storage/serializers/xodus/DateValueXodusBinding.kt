package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DateValue

/**
 * A [XodusBinding] for [DateValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DateValueXodusBinding: XodusBinding<DateValue> {
    override val type = Type.Date
    override fun entryToValue(entry: ByteIterable): DateValue = DateValue(LongBinding.compressedEntryToLong(entry))
    override fun valueToEntry(value: DateValue): ByteIterable = LongBinding.longToCompressedEntry(value.value)
}