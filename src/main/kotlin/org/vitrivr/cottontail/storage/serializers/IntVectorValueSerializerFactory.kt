package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.IntVectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.IntVectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.IntVectorValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object IntVectorValueSerializerFactory : ValueSerializerFactory<IntVectorValue> {
    override fun mapdb(size: Int) = IntVectorValueMapDBSerializer(size)
    override fun xodus(size: Int) = IntVectorValueXodusBinding(size)
}