package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.storage.serializers.mapdb.LongValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.LongValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object LongValueSerializerFactory : ValueSerializerFactory<LongValue> {
    override fun mapdb(size: Int) = LongValueMapDBSerializer
    override fun xodus(size: Int) = LongValueXodusBinding
}