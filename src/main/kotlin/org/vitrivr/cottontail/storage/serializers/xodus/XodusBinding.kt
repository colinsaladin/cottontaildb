package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.types.Value
import java.io.ByteArrayInputStream

/**
 * A [Serializer] for Xodus based [Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class XodusBinding<T: Value> : ComparableBinding() {
    abstract override fun readObject(stream: ByteArrayInputStream): T
    fun entryToValue(entry: ByteIterable): T = readObject(ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length))
}