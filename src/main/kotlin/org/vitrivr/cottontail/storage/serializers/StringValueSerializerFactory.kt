package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.storage.serializers.mapdb.StringValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.StringValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding


/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [StringValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object StringValueSerializerFactory : ValueSerializerFactory<StringValue> {
    override fun mapdb(size: Int) = StringValueMapDBSerializer
    override fun xodus(size: Int): XodusBinding<StringValue> = StringValueXodusBinding
}