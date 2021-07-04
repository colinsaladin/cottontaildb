package org.vitrivr.cottontail.database.index.pq

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.AbstractIndexTest
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.math.distance.Distances
import org.vitrivr.cottontail.functions.math.distance.VectorDistance
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*
import java.util.stream.Stream
import kotlin.collections.ArrayList
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * This is a collection of test cases to test the correct behaviour of [PQIndex] for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @param 1.2.1
 */
class PQDoubleIndexTest : AbstractIndexTest() {

    companion object {
        @JvmStatic
        fun kernels(): Stream<Distances> = Stream.of(Distances.L1, Distances.L2, Distances.L2SQUARED, Distances.INNERPRODUCT)
    }

    /** Random number generator. */
    private val random = SplittableRandom()

    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Type.Long),
        ColumnDef(
            this.entityName.column("feature"),
            Type.DoubleVector(this.random.nextInt(128, 2048))
        )
    )

    override val indexColumn: ColumnDef<DoubleVectorValue>
        get() = this.columns[1] as ColumnDef<DoubleVectorValue>

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_feature_pq")

    override val indexType: IndexType
        get() = IndexType.PQ

    /** Random number generator. */
    private var counter: Long = 0L

    @ParameterizedTest
    @MethodSource("kernels")
    @ExperimentalTime
    fun test(distance: Distances) {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        val k = 5000
        val query = DoubleVectorValue.random(this.indexColumn.type.logicalSize, this.random)
        val function = this.catalogue.functions.obtain(Signature.Closed(distance.functionName, Type.Double, arrayOf(query.type))) as VectorDistance<*>
        val context = BindingContext<Value>()
        val predicate = KnnPredicate(column = this.indexColumn, k = k, distance = function, query = context.bind(query))

        /* Obtain necessary transactions. */
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        val index = entityTx.indexForName(this.indexName)
        val indexTx = txn.getTx(index) as IndexTx

        /* Fetch results through index. */
        val indexResults = ArrayList<Record>(k)
        val indexDuration = measureTime {
            indexTx.filter(predicate).forEach { indexResults.add(it) }
        }

        /* Fetch results through full table scan. */
        val bruteForceResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(k)
        val bruteForceDuration = measureTime {
            entityTx.scan(arrayOf(this.indexColumn)).forEach {
                val vector = it[this.indexColumn]
                if (vector is DoubleVectorValue) {
                    bruteForceResults.offer(
                        ComparablePair(
                            it.tupleId,
                            function(vector)
                        )
                    )
                }
            }
        }

        /*
        * Calculate an error score for results (since this is an inexact index)
        * TODO: Good metric for testing.
        */
        var found = 0.0f
        for (i in 0 until k) {
            val hit = bruteForceResults[i]
            val index = indexResults.indexOfFirst { it.tupleId == hit.first }
            if (index != -1) {
                found += 1.0f
            }
        }
        val foundRatio = (found / k)
        log("Test done for ${distance::class.java.simpleName} and d=${this.indexColumn.type.logicalSize}! PQ took $indexDuration, brute-force took $bruteForceDuration. Found ratio: $foundRatio")
        txn.commit()
    }

    override fun nextRecord(): StandaloneRecord {
        val id = LongValue(this.counter++)
        val vector = DoubleVectorValue.random(this.indexColumn.type.logicalSize, this.random)
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}