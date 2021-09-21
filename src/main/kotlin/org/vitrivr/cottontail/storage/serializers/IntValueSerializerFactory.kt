package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.storage.serializers.mapdb.IntValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.IntValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.LongValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object IntValueSerializerFactory : ValueSerializerFactory<IntValue> {
    override fun mapdb(size: Int) = IntValueMapDBSerializer
    override fun xodus(size: Int, nullable: Boolean): XodusBinding<IntValue> = if (nullable) {
        IntValueXodusBinding.Nullable
    } else {
        IntValueXodusBinding.NonNullable
    }
}