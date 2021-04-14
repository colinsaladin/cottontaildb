package org.vitrivr.cottontail.database.queries.predicates.knn

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.math.knn.basics.DistanceKernel
import org.vitrivr.cottontail.math.knn.kernels.Distances
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A k nearest neighbour (kNN) lookup [Predicate]. It can be used to compare the distance between
 * database [Record] and given a query vector and select the closes k entries.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class KnnPredicate(val column: ColumnDef<*>, val k: Int, val distance: Distances, val hint: KnnPredicateHint? = null, val query: Binding<Value>, val weight: Binding<Value>? = null) : Predicate {

    init {
        /* Basic sanity checks. */
        check(this.k >= 0) { }
        if (this.k <= 0) throw QueryException.QuerySyntaxException("The value of k for a kNN query cannot be smaller than one (is $k)s!")
    }

    /** Columns affected by this [KnnPredicate]. */
    override val columns: Set<ColumnDef<*>>
        get() = setOf(this.column)

    /** Column generated by this [KnnPredicate]. */
    val produces: ColumnDef<*>
        get() = KnnUtilities.distanceColumnDef(this.column.name.entity())

    /** CPU cost required for applying this [KnnPredicate] to a single record. */
    override val atomicCpuCost: Float
        get() = if (this.weight == null) {
            this.distance.cost(this.column.type.logicalSize, true)
        } else {
            2 * this.distance.cost(this.column.type.logicalSize, false)
        }

    /**
     * Prepares this [KnnPredicate] for use in query execution, e.g., by executing late value binding.
     *
     * @param ctx [BindingContext] to use to resolve [Binding]s.
     * @return this [KnnPredicate]
     */
    override fun bindValues(ctx: BindingContext<Value>): KnnPredicate {
        this.query.context = ctx
        this.weight?.context = ctx
        return this
    }

    /**
     * Returns a [DistanceKernel] implementation for this [KnnPredicate].
     *
     * @return [DistanceKernel]
     */
    fun toKernel(): DistanceKernel<*> = if (this.weight != null) {
        this.distance.kernelForQueryAndWeight(this.query.value as RealVectorValue<*>, this.weight.value as RealVectorValue<*>)
    } else {
        this.distance.kernelForQuery(this.query.value as VectorValue<*>)
    }

    /**
     * Calculates and returns the digest for this [KnnPredicate]
     *
     * @return Digest of this [KnnPredicate] as [Long]
     */
    override fun digest(): Long {
        var result = this.javaClass.hashCode().toLong()
        result = 31L * result + this.hashCode()
        return result
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KnnPredicate

        if (column != other.column) return false
        if (k != other.k) return false
        if (query != other.query) return false
        if (distance != other.distance) return false
        if (weight != other.weight) return false
        if (hint != other.hint) return false
        return true
    }

    override fun hashCode(): Int {
        var result = column.hashCode()
        result = 31 * result + k
        result = 31 * result + query.hashCode()
        result = 31 * result + distance.hashCode()
        result = 31 * result + weight.hashCode()
        result = 31 * result + hint.hashCode()
        return result
    }
}