package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.Complex64VectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.Complex64VectorValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object Complex64VectorValueSerializerFactory : ValueSerializerFactory<Complex64VectorValue> {
    override fun mapdb(size: Int) = Complex64VectorValueMapDBSerializer(size)
    override fun xodus(size: Int) = Complex64VectorValueXodusBinding(size)
}