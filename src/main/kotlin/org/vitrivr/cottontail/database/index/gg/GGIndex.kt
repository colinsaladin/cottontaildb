package org.vitrivr.cottontail.database.index.gg

import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.general.Cursor
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.AbstractHDIndex
import org.vitrivr.cottontail.database.index.basics.AbstractIndex
import org.vitrivr.cottontail.database.index.basics.IndexState
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.database.index.pq.PQIndex
import org.vitrivr.cottontail.database.operations.Operation
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.functions.basics.Argument
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.math.distance.Distances
import org.vitrivr.cottontail.functions.math.distance.basics.VectorDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import org.vitrivr.cottontail.utilities.selection.MinSingleSelection
import java.util.*
import kotlin.collections.ArrayDeque
import kotlin.concurrent.withLock

/**
 * An index structure for nearest neighbour search (NNS) based on fast grouping algorithm proposed in [1].
 * Can be used for all types of [VectorValue]s (real and complex) as well as [Distances]. However,
 * the index must be built and prepared for a specific [Distances] metric.
 *
 * The algorithm is based on Fast Group Matching proposed in [1].
 *
 * References:
 * [1] Cauley, Stephen F., et al. "Fast group matching for MR fingerprinting reconstruction." Magnetic resonance in medicine 74.2 (2015): 523-528.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 3.0.0
 */
class GGIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractHDIndex(name, parent) {
    companion object {
        val LOGGER = LoggerFactory.getLogger(GGIndex::class.java)!!
    }

    /** The [PQIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(Distances.DISTANCE_COLUMN_DEF)

    /** The type of [AbstractIndex]. */
    override val type = IndexType.GG

    /** The [GGIndexConfig] used by this [GGIndex] instance. */
    override val config: GGIndexConfig = this.catalogue.environment.computeInTransaction { tx ->
        val entry = IndexCatalogueEntry.read(this.name, this.parent.parent.parent, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this.name}.")
        GGIndexConfig.fromParamsMap(entry.config)
    }

    /** False since [GGIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [GGIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    init {
        require(this.columns.size == 1) { "GGIndex only supports indexing a single column." }
        require(this.columns[0].type.vector) { "GGIndex only supports indexing of vector columns." }
    }

    /**
     * Checks if this [AbstractIndex] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is KnnPredicate
            && predicate.column == this.columns[0]
            && predicate.distance.signature.name == this.config.distance.functionName

    /**
     * Calculates the cost estimate if this [AbstractIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate) = Cost.ZERO // todo...

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [GGIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The [GGIndexConfig] used by this [GGIndex] instance. */
        override val config: GGIndexConfig
            get() {
                val entry = IndexCatalogueEntry.read(this@GGIndex.name, this@GGIndex.parent.parent.parent, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@GGIndex.name}.")
                return GGIndexConfig.fromParamsMap(entry.config)
            }

        /**
         * Rebuilds the surrounding [PQIndex] from scratch using the following, greedy grouping algorithm:
         *
         *  # Takes one dictionary element (random is probably easiest to start with)
         *  # Go through all yet ungrouped elements and find k = groupSize = numElementsTotal/numGroups most similar ones
         *  # Build mean vector of those k in the group and store as group representation
         *  # Don't do any PCA/SVD as we only have 18-25 ish dims...
         *  # Repeat with a new randomly selected element from the remaining ones until no elements remain.
         *
         *  Takes around 6h for 5000 groups on 9M vectors
         */
        override fun rebuild() = this.txLatch.withLock {

            /* Obtain some learning data for training. */
            PQIndex.LOGGER.debug("Rebuilding GG index {}", this@GGIndex.name)

            /* Load all tuple ids into a set. */
            val txn = this.context.getTx(this.dbo.parent) as EntityTx
            val remainingTids = mutableSetOf<Long>()
            txn.cursor(emptyArray()).forEach { remainingTids.add(it.tupleId) }

            /* Prepare necessary data structures. */
            val groupSize =
                ((remainingTids.size + this.config.numGroups - 1) / this.config.numGroups)  // ceildiv
            val finishedTIds = mutableSetOf<Long>()
            val random = SplittableRandom(this.config.seed)

            /* Start rebuilding the index. */
            this.clear()
            while (remainingTids.isNotEmpty()) {
                /* Randomly pick group seed value. */
                val groupSeedTid = remainingTids.elementAt(random.nextInt(remainingTids.size))
                val groupSeedValue = txn.read(groupSeedTid, this.columns)[this.columns[0]]
                if (groupSeedValue is VectorValue<*>) {
                    /* Perform kNN for group. */
                    val signature = Signature.Closed(this.config.distance.functionName, arrayOf(Argument.Typed(this.columns[0].type)), Type.Double)
                    val function = this@GGIndex.parent.parent.parent.functions.obtain(signature)
                    check(function is VectorDistance.Binary<*>) { "GGIndex rebuild failed: Function $signature is not a vector distance function." }
                    val knn = MinHeapSelection<ComparablePair<Pair<TupleId, VectorValue<*>>, DoubleValue>>(groupSize)
                    remainingTids.forEach { tid ->
                        val r = txn.read(tid, this.columns)
                        val vec = r[this.columns[0]]
                        if (vec is VectorValue<*>) {
                            val distance = function(vec)
                            if (knn.size < groupSize || knn.peek()!!.second > distance) {
                                knn.offer(ComparablePair(Pair(tid, vec), distance))
                            }
                        }
                    }

                    var groupMean = groupSeedValue.new()
                    val groupTids = mutableListOf<Long>()
                    for (i in 0 until knn.size) {
                        val element = knn[i].first
                        groupMean += element.second
                        groupTids.add(element.first)
                        check(remainingTids.remove(element.first)) { "${name.simple} processed an element that should have been removed by now." }
                        check(finishedTIds.add(element.first)) { "${name.simple} processed an element that was already processed." }
                    }
                    groupMean /= DoubleValue(knn.size)
                    //TODO: this@GGIndex.groupsStore[groupMean] = groupTids.toLongArray()
                }
            }
            this.updateState(IndexState.CLEAN)
            PQIndex.LOGGER.debug("Rebuilding GGIndex {} complete.", this@GGIndex.name)
        }

        /**
         * Updates the [GGIndex] with the provided [Operation.DataManagementOperation]s. Since incremental updates
         * are not supported, the [GGIndex] is set to stale.
         *
         * @param event The [Operation.DataManagementOperation] to process.
         */
        override fun update(event: Operation.DataManagementOperation) {
            this.updateState(IndexState.STALE)
        }

        /**
         * Clears the [GGIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            this.updateState(IndexState.STALE)
            //TODO: this@GGIndex.groupsStore.clear()
        }

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [Cursor] of all [Record]s that match the [Predicate].
         * Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Cursor] is not thread safe! It remains to the caller to close the [Cursor]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate): Cursor<Record> = object : Cursor<Record> {

            /** Cast [KnnPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is KnnPredicate && predicate.distance.signature.name == this@Tx.config.distance.functionName) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@GGIndex.name}' (GGIndex) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** List of query [VectorValue]s. Must be prepared before using the [Iterator]. */
            private val vector: VectorValue<*>

            /** The [ArrayDeque] of [StandaloneRecord] produced by this [GGIndex]. Evaluated lazily! */
            private val resultsQueue: ArrayDeque<StandaloneRecord> by lazy { prepareResults() }

            init {
                val value = this.predicate.query.value
                check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }
                this.vector = value
            }

            override fun moveNext(): Boolean = if (this.resultsQueue.isNotEmpty()) {
                this.resultsQueue.removeFirst()
                true
            } else {
                false
            }

            override fun key(): TupleId = this.resultsQueue.first().tupleId

            override fun value(): Record = this.resultsQueue.first()

            override fun close() {
                this.resultsQueue.clear()
            }

            /**
             * Executes the kNN and prepares the results to return by this [Iterator].
             */
            private fun prepareResults(): ArrayDeque<StandaloneRecord> {
                /* Scan >= 10% of entries by default */
                val considerNumGroups = (this@Tx.config.numGroups + 9) / 10
                val txn = this@Tx.context.getTx(this@GGIndex.parent) as EntityTx
                val signature = Signature.Closed(this@Tx.config.distance.functionName, arrayOf(Argument.Typed(this@Tx.columns[0].type)), Type.Double)
                val function = this@GGIndex.parent.parent.parent.functions.obtain(signature)
                check (function is VectorDistance.Binary<*>) { "Function $signature is not a vector distance function." }

                /** Phase 1): Perform kNN on the groups. */
                require(this.predicate.k < txn.maxTupleId() / this@Tx.config.numGroups * considerNumGroups) { "Value of k is too large for this index considering $considerNumGroups groups." }
                val groupKnn = MinHeapSelection<ComparablePair<LongArray, DoubleValue>>(considerNumGroups)

                LOGGER.debug("Scanning group mean signals.")
                //TODO: this@GGIndex.groupsStore.forEach {
                //    groupKnn.offer(ComparablePair(it.value, function(it.key)))
                //}


                /** Phase 2): Perform kNN on the per-group results. */
                val knn = if (this.predicate.k == 1) {
                    MinSingleSelection<ComparablePair<Long, DoubleValue>>()
                } else {
                    MinHeapSelection(this.predicate.k)
                }
                LOGGER.debug("Scanning group members.")
                for (k in 0 until groupKnn.size) {
                    for (tupleId in groupKnn[k].first) {
                        val value =
                            txn.read(tupleId, this@Tx.columns)[this@Tx.columns[0]]
                        if (value is VectorValue<*>) {
                            val distance = function(value)
                            if (knn.size < knn.k || knn.peek()!!.second > distance) {
                                knn.offer(ComparablePair(tupleId, distance))
                            }
                        }
                    }
                }

                /* Phase 3: Prepare and return list of results. */
                val queue = ArrayDeque<StandaloneRecord>(this.predicate.k)
                for (i in 0 until knn.size) {
                    queue.add(StandaloneRecord(knn[i].first, this@GGIndex.produces, arrayOf(knn[i].second)))
                }
                return queue
            }
        }

        /**
         * Range filtering is not supported [GGIndex]
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Cursor].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Cursor<Record> {
            throw UnsupportedOperationException("The UniqueHashIndex does not support ranged filtering!")
        }
    }
}