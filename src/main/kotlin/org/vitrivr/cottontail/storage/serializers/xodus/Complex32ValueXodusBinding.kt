package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.BindingUtils
import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.ByteUtil
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.ByteValue
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.Complex64Value
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

/**
 * A [XodusBinding] for [Complex32Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex32ValueXodusBinding: XodusBinding<Complex32Value> {
    override val type = Type.Complex32
    private val outputStream = LightOutputStream(8)
    private val buffer = ByteBuffer.allocate(8)
    override fun entryToValue(entry: ByteIterable): Complex32Value {
        this.buffer.clear()
        this.buffer.put(entry.bytesUnsafe)
        this.buffer.flip()
        return Complex32Value(this.buffer.float, this.buffer.float)
    }

    override fun valueToEntry(value: Complex32Value): ByteIterable {
        this.outputStream.clear()
        FloatBinding.BINDING.writeObject(this.outputStream, value.real.value)
        FloatBinding.BINDING.writeObject(this.outputStream, value.real.imaginary)
        return this.outputStream.asArrayByteIterable()
    }
}