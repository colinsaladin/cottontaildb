package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.storage.serializers.mapdb.ShortValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.ShortValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [ShortValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object ShortValueSerializerFactory : ValueSerializerFactory<ShortValue> {
    override fun mapdb(size: Int) = ShortValueMapDBSerializer
    override fun xodus(size: Int) = ShortValueXodusBinding
}