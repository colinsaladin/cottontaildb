package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value
import java.io.ByteArrayInputStream

/**
 * A [Serializer] for Xodus based [Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface XodusBinding<T: Value> {

    /** The [Type] converted by this [XodusBinding]. */
    val type: Type<T>

    /**
     * Converts a [ByteIterable] to a [Value].
     *
     * @param entry The [ByteIterable] to convert.
     * @return The resulting [Value].
     */
    fun entryToValue(entry: ByteIterable): T?

    /**
     * Converts a [Value] to a [ByteIterable].
     *
     * @param value The [Value] to convert.
     * @return The resulting [ByteIterable].
     */
    fun valueToEntry(value: T?): ByteIterable
}