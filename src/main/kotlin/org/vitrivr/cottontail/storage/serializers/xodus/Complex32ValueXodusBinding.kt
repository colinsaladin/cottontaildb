package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.Complex64Value
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [Complex32Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex32ValueXodusBinding: XodusBinding<Complex32Value>() {
    override fun readObject(stream: ByteArrayInputStream) = Complex32Value(FloatBinding.BINDING.readObject(stream), FloatBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is Complex32Value) { "Cannot serialize value of type $`object` to Complex32Value." }
        FloatBinding.BINDING.writeObject(output, `object`.real)
        FloatBinding.BINDING.writeObject(output, `object`.imaginary)
    }
}