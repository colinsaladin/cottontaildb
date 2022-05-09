package org.vitrivr.cottontail.cli.system

import com.google.gson.GsonBuilder
import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
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
class VectorizationCommand : AbstractCottontailCommand.System(name = "vectorize", help = "CottontailDB allows for " +
        "vectorization implemented through SIMD instructions with the Java Vector API. With this command you can " +
        "determine from what feature-vector dimension onwards it makes sense to use a vectorized approach " +
        "compared to the default scalar version. Usage: system vectorize") {

    private val randomVector = JDKRandomGenerator(123456789)
    private val randomQuery = JDKRandomGenerator(987654321)
    private val numberOfNNSQueries = 10
    private val numberOfVectors = 500000
    private val path = "cottontaildb-data/simd"

    private val vectorList = mutableListOf<FloatVectorValue>()
    private val queryList = mutableListOf<FloatVectorValue>()
    private var timeMap = mutableMapOf<Int, MutableMap<String, Double>>()

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
            val distanceFunction = ManhattanDistance.FloatVectorVectorized(queryList[0].type as Types.FloatVector)
            distanceFunction(queryList[i], vectorList[i])
        }
    }

    /**
     * Calculates a couple of Nearest Neighbor Searches for various dimensions and stores the average
     * time needed for both the scalar and the vectorized version inside a Map.
     */
    private fun evaluate(vectorize: Boolean) {

        val label = if (vectorize) "Vectorized" else "Scalar"

        for (i in 2 until 500 step 5) {
            vectorInit(i)
            queryInit(i)

            var time: Duration
            val times = mutableListOf<Double>()

            val distanceFunction = if (vectorize) {
                ManhattanDistance.FloatVectorVectorized(queryList[0].type as Types.FloatVector)
            } else {
                ManhattanDistance.FloatVector(queryList[0].type as Types.FloatVector)
            }

            for (j in 0 until queryList.size) {
                time = measureTime {
                    vectorList.forEach { vector ->
                        distanceFunction(queryList[j], vector)
                    }
                }

                times.add(time.toDouble(DurationUnit.SECONDS))
            }

            if (timeMap[i].isNullOrEmpty()) {
                timeMap[i] = mutableMapOf(label to times.average())
            } else {
                timeMap[i]?.put(label, times.average())
            }

        }
    }

    /**
     * This comparison function determines the dimension of feature-vectors where the vectorized version is
     * beneficial compared to the scalar version.
     */
    private fun compareVersions(valueMap: Map<Int, MutableMap<String, Double>>): Int? {
        valueMap.forEach { (dimension, map) ->
            var vectorizedTime = 0.0
            var scalarTime = 0.0
            map.forEach {(version, time) ->
                if (version == "Vectorized") {
                    vectorizedTime = time
                } else {
                    scalarTime = time
                }
            }

            if (vectorizedTime / scalarTime <= 0.9) {
                return dimension
            }
        }
        return null
    }

    override fun exec() {
        warmUp()

        val directory = File(path)
        if (!directory.exists()) {
            directory.mkdir()
        }

        evaluate(true)
        evaluate(false)

        if (compareVersions(timeMap) != null) {
            //TODO
        }

        val gson = GsonBuilder().setPrettyPrinting().create()
        val outputString = gson.toJson(timeMap)
        File("$path/performance.json").writeText(outputString)

    }
}
