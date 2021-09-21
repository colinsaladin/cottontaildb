package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.*
import jetbrains.exodus.util.ByteArraySizedInputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.values.StringValue
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [ComparableBinding] for Xodus based [StringBinding] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class StringValueXodusBinding: XodusBinding<StringValue> {

    override val type = Type.String

    /**
     * [StringValueXodusBinding] used for non-nullable values.
     */
    object NotNullable: StringValueXodusBinding() {
        override val type = Type.String
        override fun entryToValue(entry: ByteIterable): StringValue = StringValue(StringBinding.BINDING.readObject(ByteArraySizedInputStream(entry.bytesUnsafe)))
        override fun valueToEntry(value: StringValue?): ByteIterable {
            require(value != null) { "Serialization error: Value cannot be null." }
            return StringBinding.BINDING.objectToEntry(value.value)
        }
    }

    /**
     * [StringValueXodusBinding] used for non-nullable values.
     */
    object Nullable: StringValueXodusBinding() {
        /** The special value that is being interpreted as NULL for this [XodusBinding]. */
        private const val NULL_VALUE ="\u0000\u0000"
        private val NULL_VALUE_RAW = StringBinding.BINDING.objectToEntry(NULL_VALUE)

        override fun entryToValue(entry: ByteIterable): StringValue? {
            val bytesRead = entry.bytesUnsafe
            val bytesNull = NULL_VALUE_RAW.bytesUnsafe
            return if (Arrays.equals(bytesNull, bytesRead)) {
                null
            } else {
                StringValue(StringBinding.BINDING.readObject(ByteArraySizedInputStream(bytesRead)))
            }
        }
        override fun valueToEntry(value: StringValue?): ByteIterable {
            if (value?.value == NULL_VALUE) throw DatabaseException.ReservedValueException("Cannot serialize value '$value'! Value is reserved for NULL entries for type ${this.type}.")
            return if (value == null) NULL_VALUE_RAW else StringBinding.BINDING.objectToEntry(value.value)
        }
    }
}