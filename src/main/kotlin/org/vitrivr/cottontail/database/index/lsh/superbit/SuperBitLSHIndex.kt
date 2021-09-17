package org.vitrivr.cottontail.database.index.lsh.superbit

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.basics.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.IndexState
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.database.index.lsh.LSHIndex
import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.functions.math.distance.Distances
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.util.*

/**
 * Represents a LSH based index in the Cottontail DB data model. An [AbstractIndex] belongs to an [DefaultEntity]
 * and can be used to index one to many [Column]s. Usually, [AbstractIndex]es allow for faster data access.
 * They process [Predicate]s and return
 * [Recordset]s.
 *
 * @author Manuel Huerbin, Gabriel Zihlmann & Ralph Gasser
 * @version 3.0.0
 */
class SuperBitLSHIndex<T : VectorValue<*>>(name: Name.IndexName, parent: DefaultEntity) : LSHIndex<T>(name, parent) {

    companion object {
        private val LOGGER = LoggerFactory.getLogger(SuperBitLSHIndex::class.java)
        private val SUPPORTED_DISTANCES = arrayOf(Distances.COSINE.functionName, Distances.INNERPRODUCT.functionName)
    }

    /** False since [SuperBitLSHIndex] doesn't supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** False since [SuperBitLSHIndex] doesn't support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The [IndexType] of this [SuperBitLSHIndex]. */
    override val type = IndexType.LSH_SB

    /** The [SuperBitLSHIndexConfig] used by this [SuperBitLSHIndex] instance. */
    override val config: SuperBitLSHIndexConfig = this.catalogue.environment.computeInTransaction { tx ->
        val entry = IndexCatalogueEntry.read(this.name, this.parent.parent.parent, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this.name}.")
        SuperBitLSHIndexConfig.fromParamMap(entry.config)
    }

    init {
        require(this.columns.size == 1) { "SuperBitLSHIndex only supports indexing a single column." }
        require(this.columns[0].type.vector) { "SuperBitLSHIndex only support indexing of vector columns." }
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [SuperBitLSHIndex].
     * note: only use the inner product distances with normalized vectors!
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean =
        predicate is KnnPredicate
                && predicate.columns.first() == this.columns[0]
                && predicate.distance.signature.name in SUPPORTED_DISTANCES
                && (!this.config.considerImaginary || predicate.query is ComplexVectorValue<*>)

    /**
     * Calculates the cost estimate of this [SuperBitLSHIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = if (canProcess(predicate)) {
        Cost.ZERO /* TODO: Determine. */
    } else {
        Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param bucket The bucket index.
         * @param tupleId The [TupleId] for the mapping.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun addMapping(buckets: IntArray, tupleId: TupleId): Boolean {
            val keyRaw = IntegerBinding.intToCompressedEntry(0) //TODO
            val tupleIdRaw = LongBinding.longToCompressedEntry(tupleId)
            return if (this.dataStore.exists(this.context.xodusTx, keyRaw, tupleIdRaw)) {
                this.dataStore.put(this.context.xodusTx, keyRaw, tupleIdRaw)
            } else {
                false
            }
        }

        /**
         * Removes a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to remove a mapping for.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun removeMapping(buckets: IntArray, tupleId: TupleId): Boolean {
            val keyRaw = IntegerBinding.intToCompressedEntry(0) //TODO
            val valueRaw = LongBinding.longToCompressedEntry(tupleId)
            val cursor = this.dataStore.openCursor(this.context.xodusTx)
            if (cursor.getSearchBoth(keyRaw, valueRaw)) {
                return cursor.deleteCurrent()
            } else {
                return false
            }
        }

        /**
         * (Re-)builds the [SuperBitLSHIndex].
         */
        override fun rebuild() {
            LOGGER.debug("Rebuilding SB-LSH index {}", this@SuperBitLSHIndex.name)

            /* LSH. */
            val tx = this.context.getTx(this.dbo.parent) as EntityTx
            val specimen = this.acquireSpecimen(tx)
                ?: throw DatabaseException("Could not gather specimen to create index.") // todo: find better exception
            val lsh = SuperBitLSH(this@SuperBitLSHIndex.config.stages, this@SuperBitLSHIndex.config.buckets, this@SuperBitLSHIndex.config.seed, specimen, this@SuperBitLSHIndex.config.considerImaginary, this@SuperBitLSHIndex.config.samplingMethod)


            /* Locally (Re-)create index entries and sort bucket for each stage to corresponding map. */
            val local = List(config.stages) {
                MutableList(config.buckets) { mutableListOf<Long>() }
            }

            /* for every record get bucket-signature, then iterate over stages and add tid to the list of that bucket of that stage */
            tx.cursor(this@SuperBitLSHIndex.columns).forEach {
                val value = it[this.dbo.columns[0]] ?: throw DatabaseException("Could not find column for entry in index $this") // todo: what if more columns? This should never happen -> need to change type and sort this out on index creation
                if (value is VectorValue<*>) {
                    val buckets = lsh.hash(value)
                    (buckets zip local).forEach { (bucket, map) ->
                        map[bucket].add(it.tupleId)
                    }
                } else {
                    throw DatabaseException("$value is no vector column!")
                }
            }

            /* Clear existing maps. */
            //TODO: (this@SuperBitLSHIndex.maps zip local).forEach { (map, localdata) ->
            //    map.clear()
            //    localdata.forEachIndexed { bucket, tIds ->
            //        map[bucket] = tIds.toLongArray()
            //   }
            // }

            /* Update state of index. */
            this.updateState(IndexState.CLEAN)
            LOGGER.debug("Rebuilding SB-LSH index completed.")
        }

        /**
         * Updates the [SuperBitLSHIndex] with the provided [Operation.DataManagementOperation]. This method determines,
         * whether the [Record] affected by the [Operation.DataManagementOperation] should be added or updated
         *
         * @param event [Operation.DataManagementOperation]s to process.
         */
        override fun update(event: Operation.DataManagementOperation) {
            /* Update state of index. */
            this.updateState(IndexState.STALE)
        }

        /**
         * Clears the [SuperBitLSHIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() {
            /* Update state of index. */
            this.updateState(IndexState.STALE)
            // TODO: (this@SuperBitLSHIndex.maps).forEach { map ->
            //    map.clear()
            //}
        }

        /**
         * Performs a lookup through this [SuperBitLSHIndex] and returns a [Iterator] of
         * all [TupleId]s that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Iterator<Record> {

            /** Cast [KnnPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is KnnPredicate) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (LSH Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** List of [TupleId]s returned by this [Iterator]. */
            private val tupleIds: LinkedList<TupleId>

            /* Performs some sanity checks. */
            init {
                if (this.predicate.columns.first() != this@SuperBitLSHIndex.columns[0] || !(this.predicate.distance.signature.name in SUPPORTED_DISTANCES)) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@SuperBitLSHIndex.name}' (lsh-index) does not support the provided predicate.")
                }

                /* Assure correctness of query vector. */
                val value = this.predicate.query.value
                check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }

                /** Prepare SuperBitLSH data structure. */
                val lsh = SuperBitLSH(
                    this@SuperBitLSHIndex.config.stages,
                    this@SuperBitLSHIndex.config.buckets,
                    this@SuperBitLSHIndex.config.seed,
                    value,
                    this@SuperBitLSHIndex.config.considerImaginary,
                    this@SuperBitLSHIndex.config.samplingMethod
                )

                /** Prepare list of matches. */
                this.tupleIds = LinkedList<TupleId>()
                val signature = lsh.hash(value)
                for (stage in signature.indices) {
                    //TODO: for (tupleId in this@SuperBitLSHIndex.maps[stage].getValue(signature[stage])) {
                    //    this.tupleIds.offer(tupleId)
                    //}
                }
            }

            override fun hasNext(): Boolean {
                return this.tupleIds.isNotEmpty()
            }

            override fun next(): Record = StandaloneRecord(this.tupleIds.removeFirst(), this@SuperBitLSHIndex.produces, arrayOf())

        }

        /**
         * The [SuperBitLSHIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Iterator<Record> {
            throw UnsupportedOperationException("The SuperBitLSHIndex does not support ranged filtering!")
        }

        /**
         * Tries to find a specimen of the [VectorValue] in the [DefaultEntity] underpinning this [SuperBitLSHIndex]
         *
         * @param tx [DefaultEntity.Tx] used to read from [DefaultEntity]
         * @return A specimen of the [VectorValue] that should be indexed.
         */
        private fun acquireSpecimen(tx: EntityTx): VectorValue<*>? {
            for (index in 0L until tx.maxTupleId()) {
                val read = tx.read(index, this@SuperBitLSHIndex.columns)[this.dbo.columns[0]]
                if (read is VectorValue<*>) {
                    return read
                }
            }
            return null
        }
    }
}