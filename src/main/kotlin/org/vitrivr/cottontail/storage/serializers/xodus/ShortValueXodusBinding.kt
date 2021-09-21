package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.ShortBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.values.ShortValue
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [XodusBinding] for [ShortValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class ShortValueXodusBinding: XodusBinding<ShortValue>{

    override val type = Type.Short

    /**
     * [ShortValueXodusBinding] used for non-nullable values.
     */
    object NotNullable: ShortValueXodusBinding() {
        override fun entryToValue(entry: ByteIterable): ShortValue = ShortValue(ShortBinding.BINDING.readObject(ByteArrayInputStream(entry.bytesUnsafe)))
        override fun valueToEntry(value: ShortValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return ShortBinding.BINDING.objectToEntry(value.value)
        }
    }

    /**
     * [ShortValueXodusBinding] used for nullable values.
     */
    object Nullable: ShortValueXodusBinding() {
        private val NULL_VALUE = ShortBinding.BINDING.objectToEntry(Short.MIN_VALUE)
        override fun entryToValue(entry: ByteIterable): ShortValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                ShortValue(ShortBinding.BINDING.readObject(ByteArrayInputStream(bytesRead)))
            }
        }

        override fun valueToEntry(value: ShortValue?): ByteIterable {
            if (value == null) return NULL_VALUE
            if (value.value == Short.MIN_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return ShortBinding.BINDING.objectToEntry(value.value)
        }
    }
}