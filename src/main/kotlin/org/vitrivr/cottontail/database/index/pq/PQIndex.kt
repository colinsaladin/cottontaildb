package org.vitrivr.cottontail.database.index.pq

import jetbrains.exodus.bindings.LongBinding
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.basics.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.AbstractHDIndex
import org.vitrivr.cottontail.database.index.basics.IndexState
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.database.index.va.VAFIndex
import org.vitrivr.cottontail.database.index.va.signature.VAFMarks
import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import org.vitrivr.cottontail.utilities.selection.MinSingleSelection
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.util.*
import kotlin.collections.ArrayDeque
import kotlin.concurrent.withLock

/**
 * An [AbstractIndex] structure for nearest neighbor search (NNS) that uses a product quantization (PQ). Can
 * be used for all type of [VectorValue]s and distance metrics.
 *
 * TODO: Check if generalization to other distance metrics is actually valid.
 *
 * References:
 * [1] Guo, Ruiqi, et al. "Quantization based fast inner product search." Artificial Intelligence and Statistics. 2016.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 3.0.0
 */
class PQIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractHDIndex(name, parent) {

    companion object {
        val LOGGER = LoggerFactory.getLogger(PQIndex::class.java)!!

        /** Key to read/write the Marks entry. */
        val PQ_ENTRY_KEY = LongBinding.longToCompressedEntry(-1L)

        /**
         * Dynamically determines the number of subspaces for the given dimension.
         *
         * @param d The dimensionality of the vector.
         * @return Number of subspaces to use.
         */
        fun defaultNumberOfSubspaces(d: Int): Int {
            val start: Int = when {
                d == 1 -> 1
                d == 2 -> 2
                d <= 8 -> 4
                d <= 64 -> 4
                d <= 256 -> 8
                d <= 1024 -> 16
                d <= 4096 -> 32
                else -> 64
            }
            var subspaces = start
            while (subspaces < d && subspaces < Byte.MAX_VALUE) {
                if (d % subspaces == 0) {
                    return subspaces
                }
                subspaces++
            }
            return start
        }
    }

    /** The [PQIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(KnnUtilities.distanceColumnDef(this.parent.name))

    /** The type of [AbstractIndex]. */
    override val type = IndexType.PQ

    /** The [PQIndexConfig] used by this [PQIndex] instance. */
    override val config: PQIndexConfig = this.catalogue.environment.computeInTransaction { tx ->
        val entry = IndexCatalogueEntry.read(this.name, this.parent.parent.parent, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this.name}.")
        PQIndexConfig.fromParamMap(entry.config)
    }

    init {
        /** Some assumptions and sanity checks. Some are for documentation, some are cheap enough to actually keep and check. */
        require(this.columns.size == 1) { "PQIndex only supports indexing a single column." }
        require(this.config.numCentroids > 0) { "PQIndex supports a maximum number of ${this.config.numCentroids} centroids." }
        require(this.config.numCentroids <= Short.MAX_VALUE)
        require(this.config.numSubspaces > 0) { "PQIndex requires at least one centroid." }
        require(this.columns[0].type.logicalSize >= this.config.numSubspaces) { "Logical size of the column must be greater or equal than the number of subspaces." }
    }

    /** False since [PQIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [PQIndex] supports partitioning. */
    override val supportsPartitioning: Boolean = true

    /**
     * Checks if this [AbstractIndex] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) =
        predicate is KnnPredicate && predicate.column == this.columns[0]

    /**
     * Calculates the cost estimate if this [AbstractIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return [Cost] estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost {
        if (predicate !is KnnPredicate) return Cost.INVALID
        if (predicate.column != this.columns[0]) return Cost.INVALID
        return Cost(
            this.count * this.config.numSubspaces * Cost.COST_DISK_ACCESS_READ + predicate.k * predicate.column.type.logicalSize * Cost.COST_DISK_ACCESS_READ,
            this.count * (4 * Cost.COST_MEMORY_ACCESS + Cost.COST_FLOP) + predicate.k * predicate.atomicCpuCost,
            (predicate.k * this.produces.map { it.type.physicalSize }.sum()).toFloat()
        )
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [PQIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractHDIndex.Tx(context) {

        /** Internal [VAFMarks] reference. */
        private var pq: PQ? = null

        init {

        }

        /**
         * Rebuilds the surrounding [PQIndex] from scratch, thus re-creating the the [PQ] codebook
         * with a new, random sample and re-calculating all the signatures. This method can
         * take a while to complete!
         */
        override fun rebuild() = this.txLatch.withLock {
            /* Obtain some learning data for training. */
            LOGGER.debug("Rebuilding PQ index {}", this@PQIndex.name)
            val txn = this.context.getTx(this.dbo.parent) as EntityTx
            val data = this.acquireLearningData(txn)

            /* Obtain PQ data structure. */
            //TODO: this.pq = PQ.fromData(this@PQIndex.config, this@PQIndex.columns[0], data)

            /* Clear and re-generate signatures. */
            this.clear()
            txn.scan(this.dbo.columns).forEach { rec ->
                val value = rec[this@PQIndex.columns[0]]
                if (value is VectorValue<*>) {
                    //TODO: val sig = pq.getSignature(value)
                    //TODO: this.dataStore.put(this.context.xodusTx, sig, rec.tupleId.toKey())
                }
            }

            /* Update index state for index. */
            this.updateState(IndexState.CLEAN)
            VAFIndex.LOGGER.debug("Rebuilding PQIndex {} completed!", this@PQIndex.name)
        }

        /**
         * Updates this [PQIndex] with a new [Operation.DataManagementOperation]. Currently sets the [PQIndex] to stale.
         *
         * TODO: Implement write model for [PQIndex].
         *
         * @param event The [Operation.DataManagementOperation] to process.
         */
        override fun update(event: Operation.DataManagementOperation) = this.txLatch.withLock {
            this.updateState(IndexState.STALE)
        }

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate): Iterator<Record> = filterRange(predicate, 0, 1)

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate] in the given [LongRange]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator]
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int) = object : Iterator<Record> {

            /** Cast [KnnPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is KnnPredicate) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@PQIndex.name}' (PQ Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** The [PQ] instance used for this [Iterator]. */
            //TODO: private val pq = this@PQIndex.pqStore.get()

            /** Prepares [PQLookupTable]s for the given query vector(s). */
            //TODO: private val lookupTable: PQLookupTable

            /** The [ArrayDeque] of [StandaloneRecord] produced by this [VAFIndex]. Evaluated lazily! */
            private val resultsQueue: ArrayDeque<StandaloneRecord> by lazy {
                prepareResults()
            }

            /** The [IntRange] that should be scanned by this [VAFIndex]. */
            private val range: IntRange = 0 until 1

            init {
                val value = this.predicate.query.value
                check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }
                //TODO: this.lookupTable = this.pq.getLookupTable(value, this.predicate.distance)

                /* Calculate partition size. */
                // TODO: val pSize = Math.floorDiv(this@PQIndex.signaturesStore.size, partitions) + 1
                // TODO: this.range = pSize * partitionIndex until min(pSize * (partitionIndex + 1), this@PQIndex.signaturesStore.size)
            }

            override fun hasNext(): Boolean = this.resultsQueue.isNotEmpty()

            override fun next(): Record = this.resultsQueue.removeFirst()

            /**
             * Executes the kNN and prepares the results to return by this [Iterator].
             */
            private fun prepareResults(): ArrayDeque<StandaloneRecord> {
                /* Prepare data structures for NNS. */
                val txn = this@Tx.context.getTx(this@PQIndex.parent) as EntityTx
                val preKnnSize = (this.predicate.k * 1.15).toInt() /* Pre-kNN size is 15% larger than k. */
                val preKnn = MinHeapSelection<ComparablePair<LongArray, Double>>(preKnnSize)

                /* Phase 1: Perform pre-kNN based on signatures. */
                for (i in range) {
                    //TODO: val entry = this@PQIndex.signaturesStore[i]
                    //val approximation =
                    //   this.lookupTable.approximateDistance(entry!!.signature)
                    //if (preKnn.size < this.predicate.k || preKnn.peek()!!.second > approximation) {
                    //    preKnn.offer(ComparablePair(entry.tupleIds, approximation))
                    //}
                }

                /* Phase 2: Perform exact kNN based on pre-kNN results. */
                val query = this.predicate.query.value as VectorValue<*>
                val knn = if (this.predicate.k == 1) {
                    MinSingleSelection<ComparablePair<TupleId, DoubleValue>>()
                } else {
                    MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(this.predicate.k)
                }
                for (j in 0 until preKnn.size) {
                    val tupleIds = preKnn[j].first
                    for (tupleId in tupleIds) {
                        val exact = txn.read(tupleId, this@PQIndex.columns)[this@PQIndex.columns[0]]
                        if (exact is VectorValue<*>) {
                            val distance = this.predicate.distance(query, exact)
                            if (knn.size < this.predicate.k || knn.peek()!!.second > distance) {
                                knn.offer(ComparablePair(tupleId, distance))
                            }
                        }
                    }
                }

                /* Phase 3: Prepare and return list of results. */
                val queue = ArrayDeque<StandaloneRecord>(this.predicate.k)
                for (i in 0 until knn.size) {
                    queue.add(StandaloneRecord(knn[i].first, this@PQIndex.produces, arrayOf(knn[i].second)))
                }
                return queue
            }
        }

        /**
         * Collects and returns a subset of the available data for learning and training.
         *
         * @param txn The [EntityTx] used to obtain the learning data.
         * @return List of [Record]s used for learning.
         */
        private fun acquireLearningData(txn: EntityTx): List<VectorValue<*>> {
            val learningData = LinkedList<VectorValue<*>>()
            val rng = SplittableRandom(this@PQIndex.config.seed)
            val learningDataFraction = this@PQIndex.config.sampleSize.toDouble() / txn.count()
            txn.scan(this.dbo.columns).forEach {
                if (rng.nextDouble() <= learningDataFraction) {
                    val value = it[this@PQIndex.columns[0]]
                    if (value is VectorValue<*>) {
                        learningData.add(value)
                    }
                }
            }
            return learningData
        }
    }
}
