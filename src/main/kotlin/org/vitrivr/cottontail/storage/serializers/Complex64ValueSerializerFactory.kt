package org.vitrivr.cottontail.storage.serializers


import org.vitrivr.cottontail.model.values.Complex64Value
import org.vitrivr.cottontail.storage.serializers.mapdb.Complex64ValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.Complex64ValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [Complex64Value]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object Complex64ValueSerializerFactory : ValueSerializerFactory<Complex64Value> {
    override fun mapdb(size: Int) = Complex64ValueMapDBSerializer
    override fun xodus(size: Int) = Complex64ValueXodusBinding
}