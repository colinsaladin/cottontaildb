package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.*
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.values.FloatValue
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [XodusBinding] for [FloatValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class FloatValueXodusBinding: XodusBinding<FloatValue> {
    override val type = Type.Float

    /**
     * [FloatValueXodusBinding] used for nullable values.
     */
    object NonNullable: FloatValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): FloatValue =  FloatValue(FloatBinding.BINDING.readObject(ByteArrayInputStream(entry.bytesUnsafe)))
        override fun valueToEntry(value: FloatValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return FloatBinding.BINDING.objectToEntry(value.value)
        }
    }

    /**
     * [FloatValueXodusBinding] used for nullable values.
     */
    object Nullable: FloatValueXodusBinding() {
        private val NULL_VALUE = FloatBinding.BINDING.objectToEntry(Float.MIN_VALUE)

        override fun entryToValue(entry: ByteIterable): FloatValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                FloatValue(FloatBinding.BINDING.readObject(ByteArrayInputStream(bytesRead)))
            }
        }

        override fun valueToEntry(value: FloatValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Float.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return FloatBinding.BINDING.objectToEntry(value.value)
        }
    }
}