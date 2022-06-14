package org.vitrivr.cottontail.cli.system

import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import java.io.File
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to start the performance comparison between the vectorized version using the SPECIES_PREFERRED and the
 * scalar version.
 *
 * @author Colin Saladin
 */
@ExperimentalTime
class BreakEvenCommand : AbstractCottontailCommand.System(name = "break-even", help = "CottontailDB allows for " +
        "vectorization implemented through SIMD instructions with the Java Vector API. With this command you can " +
        "determine from what feature-vector dimension onwards it makes sense to use a vectorized approach " +
        "compared to the default scalar version. Usage: system break-even") {

    private val randomVector = JDKRandomGenerator(123456789)
    private val randomQuery = JDKRandomGenerator(987654321)
    private val numberOfNNSQueries = 10
    private val numberOfVectors = 500000
    private val path = "cottontaildb-data/simd"

    private val vectorList = mutableListOf<FloatVectorValue>()
    private val queryList = mutableListOf<FloatVectorValue>()

    /**
     * This method is used to initialize the vectors that will function as in-memory feature-vectors.
     */
    private fun vectorInit(dimension: Int) {
        vectorList.clear()
        for (i in 0 until numberOfVectors) {
            vectorList.add(FloatVectorValueGenerator.random(dimension, randomVector))
        }
    }

    /**
     * This method creates the query vectors that are compared to the in-memory feature-vectors.
     */
    private fun queryInit(dimension: Int) {
        queryList.clear()
        for (i in 0 until numberOfNNSQueries) {
            queryList.add(FloatVectorValueGenerator.random(dimension, randomQuery))
        }
    }

    /**
     * Short warm-up sequence to initialize caches.
     */
    private fun warmUp() {

        vectorInit(2048)
        queryInit(2048)

        for (i in 0 until 2) {
            val distanceFunction = EuclideanDistance.FloatVectorVectorized(queryList[0].type as Types.FloatVector)
            distanceFunction(queryList[i], vectorList[i])
        }
    }


    /**
     * Calculates a couple of Nearest Neighbor Searches for various dimensions and determines
     * the dimension at which it is beneficial to use the vectorized implementation
     *
     * This method calculates the dimension roughly in a first step and later on in more detail
     */
    private fun determineVectDistance(startingDimension: Int, endDimension: Int, stepSize: Int): Int? {

        if (startingDimension == 0 || startingDimension == null) {
            return null
        }

        // Variable to reduce possibility of having an outlier leading to a flawed break-even-point
        var vectorizedWasFasterBefore = false
        var returnDimension: Int? = null
        val times = mutableListOf<Double>()
        var time: Duration

        for (dimension in startingDimension until endDimension step stepSize) {
            vectorInit(dimension)
            queryInit(dimension)

            val distanceFunctionVect = EuclideanDistance.FloatVectorVectorized(queryList[0].type as Types.FloatVector)

            for (j in 0 until queryList.size) {
                time = measureTime {
                    vectorList.forEach { vector ->
                        distanceFunctionVect(queryList[j], vector)
                    }
                }

                times.add(time.toDouble(DurationUnit.SECONDS))
            }

            val vectorizedTime = times.average()
            times.clear()

            val distanceFunctionScalar = EuclideanDistance.FloatVector(queryList[0].type as Types.FloatVector)

            for (j in 0 until queryList.size) {
                time = measureTime {
                    vectorList.forEach { vector ->
                        distanceFunctionScalar(queryList[j], vector)
                    }
                }

                times.add(time.toDouble(DurationUnit.SECONDS))
            }

            val scalarTime = times.average()
            times.clear()

            if (vectorizedWasFasterBefore && vectorizedTime / scalarTime <= 0.9) {
                println("Vect is better again at $dimension")
                return returnDimension
            } else if (vectorizedTime / scalarTime <= 0.9) {
                println("Vect is better at $dimension")
                vectorizedWasFasterBefore = true
                returnDimension = dimension
            } else {
                println("Scalar still better at $dimension")
                vectorizedWasFasterBefore = false
            }
        }

        return returnDimension
    }

    override fun exec() {
        warmUp()

        val directory = File(path)
        if (!directory.exists()) {
            directory.mkdir()
        }

        var breakEvenDimension = determineVectDistance(2, 2048, 50)

        if (breakEvenDimension == null) {
            println("Break-even dimension was null" )
        } else if (breakEvenDimension == 2) {
            println("Always vectorizing? ")
        } else {
            println("Fine tuning begins...")
            breakEvenDimension = determineVectDistance(breakEvenDimension - 50, breakEvenDimension, 1)
        }

        println(breakEvenDimension)
        // TODO : Write value to config file

        /*val gson = GsonBuilder().setPrettyPrinting().create()
        val outputString = gson.toJson(timeMap)
        File("$path/performance.json").writeText(outputString)*/

    }
}
