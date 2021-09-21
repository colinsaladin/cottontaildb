package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.storage.serializers.mapdb.DoubleValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.DoubleValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.FloatValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [DoubleValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object DoubleValueSerializerFactory : ValueSerializerFactory<DoubleValue> {
    override fun mapdb(size: Int) = DoubleValueMapDBSerializer
    override fun xodus(size: Int, nullable: Boolean): XodusBinding<DoubleValue> = if (nullable) {
        DoubleValueXodusBinding.Nullable
    } else {
        DoubleValueXodusBinding.NonNullable
    }
}