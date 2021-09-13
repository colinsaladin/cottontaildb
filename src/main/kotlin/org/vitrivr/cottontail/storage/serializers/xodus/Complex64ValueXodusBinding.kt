package org.vitrivr.cottontail.storage.serializers.xodus

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.DoubleBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.values.Complex64Value
import java.io.ByteArrayInputStream

/**
 * A [ComparableBinding] for Xodus based [Complex64Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex64ValueXodusBinding: XodusBinding<Complex64Value>() {
    override fun readObject(stream: ByteArrayInputStream) = Complex64Value(DoubleBinding.BINDING.readObject(stream), DoubleBinding.BINDING.readObject(stream))
    override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
        require(`object` is Complex64Value) { "Cannot serialize value of type $`object` to Complex64Value." }
        DoubleBinding.BINDING.writeObject(output, `object`.real)
        DoubleBinding.BINDING.writeObject(output, `object`.imaginary)
    }
}