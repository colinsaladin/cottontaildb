package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.ByteValue
import org.vitrivr.cottontail.storage.serializers.mapdb.ByteValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.ByteValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object ByteValueSerializerFactory : ValueSerializerFactory<ByteValue> {
    override fun mapdb(size: Int) = ByteValueMapDBSerializer
    override fun xodus(size: Int) = ByteValueXodusBinding
}