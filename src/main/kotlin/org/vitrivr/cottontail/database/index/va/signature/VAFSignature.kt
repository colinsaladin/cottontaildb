package org.vitrivr.cottontail.database.index.va.signature

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import java.io.ByteArrayInputStream


/**
 * A fixed length [VAFSignature] used for vector approximation.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class VAFSignature(val cells: IntArray): Comparable<VAFSignature> {
    /**
     * Accessor for [VAFSignature].
     *
     * @param index The [index] to access.
     * @return [Int] value at the given [index].
     */
    operator fun get(index: Int): Int = this.cells[index]

    companion object : ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream) = VAFSignature(IntArray(IntegerBinding.readCompressed(stream)) {
            IntegerBinding.readCompressed(stream)
        })

        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is VAFSignature) { "Cannot serialize object $`object` as VAFSignature." }
            IntegerBinding.writeCompressed(output, `object`.cells.size)
            for (b in `object`.cells) {
                IntegerBinding.writeCompressed(output, b)
            }
        }
    }

    override fun compareTo(other: VAFSignature): Int {
        for ((i,b) in this.cells.withIndex()) {
            if (i >= other.cells.size) return Int.MIN_VALUE
            val comp = b.compareTo(other.cells[i])
            if (comp != 0)  return comp
        }
        return 0
    }
}