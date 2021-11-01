package org.vitrivr.cottontail.database.index.va

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Cursor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnTx
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.*
import org.vitrivr.cottontail.database.index.va.bounds.Bounds
import org.vitrivr.cottontail.database.index.va.bounds.L1Bounds
import org.vitrivr.cottontail.database.index.va.bounds.L2Bounds
import org.vitrivr.cottontail.database.index.va.bounds.L2SBounds
import org.vitrivr.cottontail.database.index.va.signature.VAFMarks
import org.vitrivr.cottontail.database.index.va.signature.VAFSignature
import org.vitrivr.cottontail.database.operations.Operation
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.statistics.columns.DoubleVectorValueStatistics
import org.vitrivr.cottontail.database.statistics.columns.FloatVectorValueStatistics
import org.vitrivr.cottontail.database.statistics.columns.IntVectorValueStatistics
import org.vitrivr.cottontail.database.statistics.columns.LongVectorValueStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.functions.math.distance.Distances
import org.vitrivr.cottontail.functions.math.distance.basics.MinkowskiDistance
import org.vitrivr.cottontail.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.basics.toKey
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.lang.Math.floorDiv
import kotlin.concurrent.withLock
import kotlin.math.max
import kotlin.math.min

/**
 * An [AbstractIndex] structure for nearest neighbor search (NNS) that uses a vector approximation (VA) file ([1]).
 * Can be used for all types of [RealVectorValue]s and all Minkowski metrics (L1, L2 etc.).
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 3.0.0
 */
class VAFIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractHDIndex(name, parent) {

    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(VAFIndex::class.java)

        /** Key to read/write the Marks entry. */
        val MARKS_ENTRY_KEY = LongBinding.longToCompressedEntry(0L)
    }

    /** The [VAFIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(this.columns.first(), Distances.DISTANCE_COLUMN_DEF)

    /** The type of [AbstractIndex]. */
    override val type = IndexType.VAF

    /** The [VAFIndexConfig] used by this [VAFIndex] instance. */
    override val config: VAFIndexConfig
        get() = this.catalogue.environment.computeInTransaction { tx ->
            val entry = IndexCatalogueEntry.read(this.name, this.parent.parent.parent, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this.name}.")
            VAFIndexConfig.fromParamMap(entry.config)
        }

    /** False since [VAFIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [VAFIndex] supports partitioning. */
    override val supportsPartitioning: Boolean = false

    /**
     * Calculates the cost estimate if this [VAFIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost {
        if (predicate !is KnnPredicate) return Cost.INVALID
        if (predicate.column != this.columns[0]) return Cost.INVALID
        if (predicate.distance !is MinkowskiDistance<*>) return Cost.INVALID
        return Cost(
            this.count * (0.9f * Cost.COST_DISK_ACCESS_READ + 0.1f * this.columns[0].type.physicalSize * Cost.COST_DISK_ACCESS_READ),
            this.count * (0.9f * (2 * Cost.COST_MEMORY_ACCESS + Cost.COST_FLOP) + 0.1f * predicate.atomicCpuCost)
        )
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAFIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is KnnPredicate && predicate.column == this.columns[0]

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [VAFIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractHDIndex.Tx(context) {

        /** Internal [VAFMarks] reference. */
        private var marks: VAFMarks

        init {
            val rawMarksEntry = this.dataStore.get(this.context.xodusTx, MARKS_ENTRY_KEY)
            if (rawMarksEntry == null) {
                this.marks = VAFMarks.getEquidistantMarks(DoubleArray(this.dimension), DoubleArray(this.dimension), IntArray(this.dimension) { this.config.marksPerDimension })
            } else {
                this.marks = VAFMarks.entryToObject(rawMarksEntry) as VAFMarks
            }
        }

        /** The configuration map used for the [Index] that underpins this [IndexTx]. */
        override val config: VAFIndexConfig
            get() {
                val entry = IndexCatalogueEntry.read(this@VAFIndex.name, this@VAFIndex.parent.parent.parent, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@VAFIndex.name}.")
                return VAFIndexConfig.fromParamMap(entry.config)
            }

        /**
         * (Re-)builds the [VAFIndex] from scratch.
         */
        override fun rebuild() = this.txLatch.withLock {
            LOGGER.debug("Rebuilding VAF index {}", this@VAFIndex.name)

            /* Prepare transaction for entity. */
            val entityTx = this.context.getTx(this@VAFIndex.parent) as EntityTx
            val columnTx = this.context.getTx(entityTx.columnForName(this.columns[0].name)) as ColumnTx<*>

            /* Obtain minimum and maximum per dimension. */
            val stat = columnTx.statistics()
            val min = DoubleArray(columns[0].type.logicalSize)
            val max = DoubleArray(columns[0].type.logicalSize)
            when (stat) {
                is FloatVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it].toDouble()
                    max[it] = stat.max.data[it].toDouble()
                }
                is DoubleVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it]
                    max[it] = stat.max.data[it]
                }
                is IntVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it].toDouble()
                    max[it] = stat.max.data[it].toDouble()
                }
                is LongVectorValueStatistics -> repeat(min.size) {
                    min[it] = stat.min.data[it].toDouble()
                    max[it] = stat.max.data[it].toDouble()
                }
                else -> {
                    /* Brute force :-( This may take a while. */
                    entityTx.cursor(this.columns).forEach { r ->
                        val value = r[this.columns[0]] as VectorValue<*>
                        for (i in 0 until value.logicalSize) {
                            min[i] = min(min[i], value[i].asDouble().value)
                            max[i] = max(max[i], value[i].asDouble().value)
                        }
                    }
                }
            }

            /* Calculate and update marks. */
            this.marks = VAFMarks.getEquidistantMarks(min, max, IntArray(this.columns[0].type.logicalSize) { this.config.marksPerDimension })
            this.dataStore.put(this.context.xodusTx, MARKS_ENTRY_KEY, VAFMarks.objectToEntry(this.marks))

            /* Calculate and update signatures. */
            this.clear()
            entityTx.cursor(this.columns).forEach { r ->
                val value = r[this.columns[0]]
                if (value is RealVectorValue<*>) {
                    this.dataStore.put(this.context.xodusTx, r.tupleId.toKey(), VAFSignature.valueToEntry(this.marks.getSignature(value)))
                }
            }

            /* Update catalogue entry for index. */
            this.updateState(IndexState.CLEAN)
        }

        /**
         * Updates this [VAFIndex] with a new [Operation.DataManagementOperation]. Currently sets the [VAFIndex] to stale.
         *
         * TODO: Implement write model for [VAFIndex].
         *
         * @param event The [Operation.DataManagementOperation] to process.
         */
        override fun update(event: Operation.DataManagementOperation) = this.txLatch.withLock {
            this.updateState(IndexState.STALE)
        }

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = filterRange(predicate, 0, 1)

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate] within the given [LongRange]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int) = object : org.vitrivr.cottontail.database.general.Cursor<Record> {

            /** Cast  to [KnnPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is KnnPredicate) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@VAFIndex.name}' (VAF Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** [VectorValue] used for query. Must be prepared before using the [Iterator]. */
            private val query: RealVectorValue<*>

            /** The [VAFMarks] used by this [Iterator]. */
            private val marks: VAFMarks = this@Tx.marks

            /** The [Bounds] objects used for filtering. */
            private val bounds: Bounds

            /** Creates a read-only snapshot of the enclosing Tx. */
            private val subTx = this@Tx.context.xodusTx.readonlySnapshot

            /** The [Cursor] used to read [VAFSignature]s. */
            private val cursor: Cursor = this@Tx.dataStore.openCursor(this.subTx)

            /** The maximum [TupleId] to iterate to (as ArrayByteIterable). */
            private val endKey: ArrayByteIterable

            /** Internal [EntityTx] used to access actual values. */
            private val entityTx = this@Tx.context.getTx(this@VAFIndex.parent) as EntityTx

            /** The number of entries read by this [Cursor] (i.e. the number of entries that have been accessed). */
            private var read = 0L

            /** */
            private var distance: DoubleValue = DoubleValue(Double.MAX_VALUE)

            /** */
            private var value: VectorValue<*>? = null

            /** */
            private var selection = MinHeapSelection<Double>(this.predicate.k)

            init {
                val value = this.predicate.query.value
                check(value is RealVectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }
                this.query = value
                this.bounds = when (this.predicate.distance) {
                    is ManhattanDistance<*> -> L1Bounds(this.query, this.marks)
                    is EuclideanDistance<*> -> L2Bounds(this.query, this.marks)
                    is SquaredEuclideanDistance<*> -> L2SBounds(this.query, this.marks)
                    else -> throw IllegalArgumentException("The ${this.predicate.distance} distance kernel is not supported by VAFIndex.")
                }

                /* Calculate partition size. */
                val pSize = floorDiv(this@Tx.count(), partitions) + 1
                this.cursor.getSearchKey((pSize * partitionIndex).toKey())
                this.endKey = min(pSize * (partitionIndex + 1), this@Tx.count()).toKey()
            }

            /**
             * Moves the internal cursor and return true, as long as new candidates appear.
             */
            override fun moveNext(): Boolean {
                while (this.cursor.next && this.cursor.key < this.endKey) {
                    if (this.selection.size < this.predicate.k || this.bounds.isVASSACandidate(VAFSignature.entryToValue(this.cursor.value), this.selection.peek()!!)) {
                        val tupleId = this.key()
                        this.value = this.entityTx.read(tupleId, this@VAFIndex.columns)[this@VAFIndex.columns[0]] as VectorValue<*>
                        this.distance = this.predicate.distance(this.query, this.value!!)
                        this.selection.offer(this.distance.value)
                        return true
                    }
                }
                return false
            }

            override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)

            override fun value(): Record = StandaloneRecord(this.key(), this@VAFIndex.produces, arrayOf(this.value, this.distance))

            /**
             * Closes this [Cursor]
             */
            override fun close() {
                this.cursor.close()
                this.subTx.abort()
            }
        }
    }
}