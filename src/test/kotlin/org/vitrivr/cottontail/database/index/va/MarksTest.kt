package org.vitrivr.cottontail.database.index.va

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.database.index.va.signature.VAFMarks
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import java.util.*

/**
 * Unit Test that can be used to test the [VAFMarks] implementation.
 *
 * @author Gabriel Zihlmann
 * @version 1.0.0
 */
internal class MarksTest {
    val random = SplittableRandom()
    val numVecs = 100
    val numDim = 20
    val marksPerDim = 100
    val realdata = Array(numVecs) {
        DoubleVectorValue.random(this.numDim, this.random)
    }
    val arrays = realdata.map { it.data }.toTypedArray()

    @Test
    fun getCells() {
        val marks: VAFMarks =
            MarksGenerator.getEquidistantMarks(arrays, IntArray(numDim) { marksPerDim })
        this.realdata.forEach {
            marks.getSignature(it).forEachIndexed { i, m ->
                assertTrue(it.data[i] >= marks.marks[i][m])
                assertTrue(it.data[i] <= marks.marks[i][m + 1])
            }
        }
    }
}