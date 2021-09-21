package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.storage.serializers.mapdb.BooleanValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.BooleanValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [BooleanValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object BooleanValueSerializerFactory : ValueSerializerFactory<BooleanValue> {
    override fun mapdb(size: Int) = BooleanValueMapDBSerializer
    override fun xodus(size: Int, nullable: Boolean): XodusBinding<BooleanValue> = if (nullable) {
        BooleanValueXodusBinding.Nullable
    } else {
        BooleanValueXodusBinding.NonNullable
    }
}