package org.vitrivr.cottontail.functions.math.distance

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.functions.math.distance.binary.*
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type

/**
 * An enumeration of all [Distances] supported by Cottontail DB for NNS.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
enum class Distances(val functionName: Name.FunctionName) {
    L1(ManhattanDistance.Generator.FUNCTION_NAME),
    L2(EuclideanDistance.Generator.FUNCTION_NAME),
    L2SQUARED(SquaredEuclideanDistance.Generator.FUNCTION_NAME),
    HAMMING(HammingDistance.Generator.FUNCTION_NAME),
    COSINE(CosineDistance.Generator.FUNCTION_NAME),
    CHISQUARED(ChisquaredDistance.Generator.FUNCTION_NAME),
    INNERPRODUCT(InnerProductDistance.Generator.FUNCTION_NAME),
    HAVERSINE(HaversineDistance.Generator.FUNCTION_NAME);

    companion object {
        /** The default distance [Name.ColumnName]. */
        val DISTANCE_COLUMN_NAME = Name.ColumnName("distance")

        /** The default distance [ColumnDef]. */
        val DISTANCE_COLUMN_DEF = ColumnDef(DISTANCE_COLUMN_NAME, Type.Double, false, false)
    }
}