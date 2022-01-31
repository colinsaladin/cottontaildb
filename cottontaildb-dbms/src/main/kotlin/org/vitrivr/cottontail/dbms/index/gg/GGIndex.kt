package org.vitrivr.cottontail.dbms.index.gg

import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.VectorDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.functions.math.distance.Distances
import org.vitrivr.cottontail.dbms.index.AbstractIndex
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.storage.serializers.ValueSerializerFactory
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import org.vitrivr.cottontail.utilities.selection.MinSingleSelection
import java.nio.file.Path
import java.util.*
import kotlin.collections.ArrayDeque

/**
 * An index structure for nearest neighbour search (NNS) based on fast grouping algorithm proposed in [1].
 * Can be used for for all types of [VectorValue]s (real and complex) as well as [Distances]. However,
 * the index must be built and prepared for a specific [Distances] metric.
 *
 * The algorithm is based on Fast Group Matching proposed in [1].
 *
 * References:
 * [1] Cauley, Stephen F., et al. "Fast group matching for MR fingerprinting reconstruction." Magnetic resonance in medicine 74.2 (2015): 523-528.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 2.1.0
 */
class GGIndex(path: Path, parent: DefaultEntity, config: GGIndexConfig? = null) : AbstractIndex(path, parent) {
    companion object {
        const val GG_INDEX_NAME = "cdb_gg_means"
        val LOGGER = LoggerFactory.getLogger(GGIndex::class.java)!!
    }

    /** The [PQIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(KnnUtilities.distanceColumnDef(this.parent.name))

    /** The type of [AbstractIndex]. */
    override val type = IndexType.GG

    /** The [GGIndexConfig] used by this [GGIndex] instance. */
    override val config: GGIndexConfig

    /** Store of the groups mean vector and the associated [TupleId]s. */
    private val groupsStore: HTreeMap<VectorValue<*>, LongArray> = this.store.hashMap(
        GG_INDEX_NAME,
        ValueSerializerFactory.mapdb( this.columns[0].type) as Serializer<VectorValue<*>>,
        Serializer.LONG_ARRAY
    ).counterEnable().createOrOpen()

    /** False since [GGIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [GGIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    init {
        require(this.columns.size == 1) { "GGIndex only supports indexing a single column." }

        /* Load or create config. */
        val configOnDisk =
            this.store.atomicVar(GG_INDEX_NAME, GGIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                this.config = config
            } else {
                this.config = GGIndexConfig(50, System.currentTimeMillis(), Distances.L2)
            }
            configOnDisk.set(this.config)
        } else {
            this.config = configOnDisk.get()
        }
        this.store.commit()
    }

    /**
     * Checks if this [AbstractIndex] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is ProximityPredicate.NNS
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
    override fun newTx(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: org.vitrivr.cottontail.dbms.execution.TransactionContext) : AbstractIndex.Tx(context) {
        /**
         * Returns the number of groups in this [GGIndex]
         *
         * @return The number of groups stored in this [GGIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@GGIndex.groupsStore.size.toLong()
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
        override fun rebuild() = this.withWriteLock {

            /* Obtain some learning data for training. */
            PQIndex.LOGGER.debug("Rebuilding GG index {}", this@GGIndex.name)

            /* Load all tuple ids into a set. */
            val txn = this.context.getTx(this.dbo.parent) as EntityTx
            val remainingTids = mutableSetOf<Long>()
            txn.scan(emptyArray()).forEach { remainingTids.add(it.tupleId) }

            /* Prepare necessary data structures. */
            val groupSize =
                ((remainingTids.size + config.numGroups - 1) / config.numGroups)  // ceildiv
            val finishedTIds = mutableSetOf<Long>()
            val random = SplittableRandom(this@GGIndex.config.seed)

            /* Start rebuilding the index. */
            this@GGIndex.groupsStore.clear()
            while (remainingTids.isNotEmpty()) {
                /* Randomly pick group seed value. */
                val groupSeedTid = remainingTids.elementAt(random.nextInt(remainingTids.size))
                val groupSeedValue = txn.read(groupSeedTid, this@GGIndex.columns)[this@GGIndex.columns[0]]
                if (groupSeedValue is VectorValue<*>) {
                    /* Perform kNN for group. */
                    val signature = Signature.Closed(this@GGIndex.config.distance.functionName, arrayOf(Argument.Typed(this@GGIndex.columns[0].type), Argument.Typed(this@GGIndex.columns[0].type)), Types.Double)
                    val function = this@GGIndex.parent.parent.parent.functions.obtain(signature)
                    check(function is VectorDistance<*>) { "GGIndex rebuild failed: Function $signature is not a vector distance function." }
                    val knn = MinHeapSelection<ComparablePair<Pair<TupleId, VectorValue<*>>, DoubleValue>>(groupSize)
                    remainingTids.forEach { tid ->
                        val r = txn.read(tid, this@GGIndex.columns)
                        val queryVector = r[this@GGIndex.columns[0]]
                        if (queryVector is VectorValue<*>) {
                            val distance = function(groupSeedValue, queryVector)!!
                            if (knn.size < groupSize || knn.peek()!!.second > distance) {
                                knn.offer(ComparablePair(Pair(tid, queryVector), distance))
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
                    this@GGIndex.groupsStore[groupMean] = groupTids.toLongArray()
                }
            }
            this@GGIndex.dirtyField.compareAndSet(true, false)
            PQIndex.LOGGER.debug("Rebuilding GGIndex {} complete.", this@GGIndex.name)
        }

        /**
         * Updates the [GGIndex] with the provided [Operation.DataManagementOperation]s. This method determines,
         * whether the [Record] affected by the [Operation.DataManagementOperation] should be added or updated
         *
         * @param event [Operation.DataManagementOperation]s to process.
         */
        override fun update(event: Operation.DataManagementOperation) = this.withWriteLock {
            this@GGIndex.dirtyField.compareAndSet(false, true)
            Unit
        }

        /**
         * Clears the [GGIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.withWriteLock {
            this@GGIndex.dirtyField.compareAndSet(false, true)
            this@GGIndex.groupsStore.clear()
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
        override fun filter(predicate: Predicate): Iterator<Record> = object : Iterator<Record> {

            /** Cast [KnnPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is ProximityPredicate.NNS && predicate.distance.signature.name == this@GGIndex.config.distance.functionName) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@GGIndex.name}' (GGIndex) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** List of query [VectorValue]s. Must be prepared before using the [Iterator]. */
            private val vector: VectorValue<*>

            /** The [ArrayDeque] of [StandaloneRecord] produced by this [GGIndex]. Evaluated lazily! */
            private val resultsQueue: ArrayDeque<StandaloneRecord> by lazy { prepareResults() }

            init {
                this@Tx.withReadLock { }
                val value = this.predicate.query.value
                check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }
                this.vector = value
            }

            override fun hasNext(): Boolean = this.resultsQueue.isNotEmpty()

            override fun next(): Record = this.resultsQueue.removeFirst()

            /**
             * Executes the kNN and prepares the results to return by this [Iterator].
             */
            private fun prepareResults(): ArrayDeque<StandaloneRecord> {
                val queue = ArrayDeque<StandaloneRecord>(this.predicate.k)

                /* Scan >= 10% of entries by default */
                val considerNumGroups = (this@GGIndex.config.numGroups + 9) / 10
                val txn = this@Tx.context.getTx(this@GGIndex.parent) as EntityTx
                val signature = Signature.Closed(this@GGIndex.config.distance.functionName, arrayOf(Argument.Typed(this@GGIndex.columns[0].type)), Types.Double)
                val function = this@GGIndex.parent.parent.parent.functions.obtain(signature)
                check (function is VectorDistance<*>) { "Function $signature is not a vector distance function." }

                /** Phase 1): Perform kNN on the groups. */
                require(this.predicate.k < txn.maxTupleId() / config.numGroups * considerNumGroups) { "Value of k is too large for this index considering $considerNumGroups groups." }
                val groupKnn = MinHeapSelection<ComparablePair<LongArray, DoubleValue>>(considerNumGroups)

                LOGGER.debug("Scanning group mean signals.")
                val query = this.predicate.query.value ?: return queue
                this@GGIndex.groupsStore.forEach {
                    groupKnn.offer(ComparablePair(it.value, function(query, it.key)!!))
                }

                /** Phase 2): Perform kNN on the per-group results. */
                val knn = if (this.predicate.k == 1) {
                    MinSingleSelection<ComparablePair<Long, DoubleValue>>()
                } else {
                    MinHeapSelection(this.predicate.k)
                }
                LOGGER.debug("Scanning group members.")
                for (k in 0 until groupKnn.size) {
                    for (tupleId in groupKnn[k].first) {
                        val probingArgument = txn.read(tupleId, this@GGIndex.columns)[this@GGIndex.columns[0]] /* Probing argument is dynamic. */
                        if (probingArgument is VectorDistance<*>) {
                            val distance = function(query, probingArgument)!!
                            if (knn.size < knn.k || knn.peek()!!.second > distance) {
                                knn.offer(ComparablePair(tupleId, distance))
                            }
                        }
                    }
                }

                /* Phase 3: Prepare and return list of results. */
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
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Iterator<Record> {
            throw UnsupportedOperationException("The UniqueHashIndex does not support ranged filtering!")
        }
    }
}