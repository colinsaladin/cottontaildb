package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.FloatValue
import org.vitrivr.cottontail.storage.serializers.mapdb.FloatValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.FloatValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [FloatValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object FloatValueSerializerFactory : ValueSerializerFactory<FloatValue> {
    override fun mapdb(size: Int) = FloatValueMapDBSerializer
    override fun xodus(size: Int) = FloatValueXodusBinding
}