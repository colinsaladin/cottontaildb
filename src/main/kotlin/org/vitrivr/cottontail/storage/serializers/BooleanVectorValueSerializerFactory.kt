package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.BooleanVectorValue
import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.BooleanVectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.BooleanVectorValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.Complex32VectorValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object BooleanVectorValueSerializerFactory : ValueSerializerFactory<BooleanVectorValue> {
    override fun mapdb(size: Int) = BooleanVectorValueMapDBSerializer(size)
    override fun xodus(size: Int, nullable: Boolean): XodusBinding<BooleanVectorValue> = if (nullable) {
        BooleanVectorValueXodusBinding.Nullable(size)
    } else {
        BooleanVectorValueXodusBinding.NonNullable(size)
    }
}