package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.BooleanVectorValue

/**
 * A [ComparableBinding] for Xodus based [BooleanVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BooleanVectorValueXodusBinding(size: Int): XodusBinding<BooleanVectorValue> {
    init {
        require(size > 0) { "Cannot initialize vector value binding with size value of $size." }
    }

    override val type: Type<BooleanVectorValue> = Type.BooleanVector(size)

    /**
     * [BooleanVectorValueXodusBinding] used for non-nullable values.
     */
    class NonNullable(size: Int): BooleanVectorValueXodusBinding(size)  {
        override fun entryToValue(entry: ByteIterable): BooleanVectorValue {
            TODO("Not yet implemented")
        }

        override fun valueToEntry(value: BooleanVectorValue?): ByteIterable {
            TODO("Not yet implemented")
        }
    }

    /**
     * [BooleanVectorValueXodusBinding] used for nullable values.
     */
    class Nullable(size: Int): BooleanVectorValueXodusBinding(size)  {
        override fun entryToValue(entry: ByteIterable): BooleanVectorValue {
            TODO("Not yet implemented")
        }

        override fun valueToEntry(value: BooleanVectorValue?): ByteIterable {
            TODO("Not yet implemented")
        }
    }
}