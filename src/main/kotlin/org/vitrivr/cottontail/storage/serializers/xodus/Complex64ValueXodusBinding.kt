package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.Complex64Value
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

/**
 * A [XodusBinding] for [Complex64Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex64ValueXodusBinding: XodusBinding<Complex64Value> {
    override val type = Type.Complex64
    private val outputStream = LightOutputStream(16)
    private val buffer = ByteBuffer.allocate(16)
    override fun entryToValue(entry: ByteIterable): Complex64Value {
        this.buffer.clear()
        this.buffer.put(entry.bytesUnsafe)
        this.buffer.flip()
        return Complex64Value(this.buffer.double, this.buffer.double)
    }

    override fun valueToEntry(value: Complex64Value): ByteIterable {
        this.outputStream.clear()
        FloatBinding.BINDING.writeObject(this.outputStream, value.real.value)
        FloatBinding.BINDING.writeObject(this.outputStream, value.real.imaginary)
        return this.outputStream.asArrayByteIterable()
    }
}