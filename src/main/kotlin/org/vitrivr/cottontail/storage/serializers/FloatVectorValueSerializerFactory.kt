package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.IntVectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.FloatVectorMapDBValueSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.FloatVectorValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.IntVectorValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object FloatVectorValueSerializerFactory : ValueSerializerFactory<FloatVectorValue> {
    override fun mapdb(size: Int) = FloatVectorMapDBValueSerializer(size)
    override fun xodus(size: Int, nullable: Boolean): XodusBinding<FloatVectorValue> = if (nullable) {
        FloatVectorValueXodusBinding.Nullable(size)
    } else {
        FloatVectorValueXodusBinding.NonNullable(size)
    }
}