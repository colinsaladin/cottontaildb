package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.Complex32VectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.Complex32VectorValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.Complex64VectorValueXodusBinding
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [Complex32VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object Complex32VectorValueSerializerFactory : ValueSerializerFactory<Complex32VectorValue> {
    override fun mapdb(size: Int) = Complex32VectorValueMapDBSerializer(size)
    override fun xodus(size: Int, nullable: Boolean): XodusBinding<Complex32VectorValue> = if (nullable) {
        Complex32VectorValueXodusBinding.Nullable(size)
    } else {
        Complex32VectorValueXodusBinding.NonNullable(size)
    }
}