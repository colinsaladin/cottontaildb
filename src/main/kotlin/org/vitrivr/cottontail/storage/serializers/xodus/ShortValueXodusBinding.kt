package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ShortBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.ShortValue

/**
 * A [XodusBinding] for [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ShortValueXodusBinding: XodusBinding<ShortValue>{
    override val type = Type.Short
    override fun entryToValue(entry: ByteIterable): ShortValue = ShortValue(ShortBinding.BINDING.entryToObject(entry) as Short)
    override fun valueToEntry(value: ShortValue): ByteIterable = ShortBinding.BINDING.objectToEntry(value.value)
}