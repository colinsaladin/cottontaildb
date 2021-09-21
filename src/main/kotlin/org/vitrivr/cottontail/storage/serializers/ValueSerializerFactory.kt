package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializer implementations for its different storage engines.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface ValueSerializerFactory<T : Value> {
    /** Reference to the [MapDBSerializer] serializer used for de-/serialization to MapDB based storage. */
    fun mapdb(size: Int = 1): MapDBSerializer<T>

    /** Reference to the [XodusBinding] used for de-/serialization of nullable values to Xodus based storage. */
    fun xodus(size: Int = 1, nullable: Boolean): XodusBinding<T>
}