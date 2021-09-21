package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.DateValue
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.storage.serializers.mapdb.DateValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.DateValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.DoubleValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [DateValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object DateValueSerializerFactory : ValueSerializerFactory<DateValue> {
    override fun mapdb(size: Int) = DateValueMapDBSerializer
    override fun xodus(size: Int, nullable: Boolean): XodusBinding<DateValue> = if (nullable) {
        DateValueXodusBinding.Nullable
    } else {
        DateValueXodusBinding.NonNullable
    }
}