package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.LongVectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.LongVectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.LongVectorValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object LongVectorValueSerializerFactory : ValueSerializerFactory<LongVectorValue> {
    override fun mapdb(size: Int) = LongVectorValueMapDBSerializer(size)
    override fun xodus(size: Int, nullable: Boolean): XodusBinding<LongVectorValue> = if (nullable) {
        LongVectorValueXodusBinding.Nullable(size)
    } else {
        LongVectorValueXodusBinding.NonNullable(size)
    }
}