package org.vitrivr.cottontail.dbms.index.hash

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.pattern.LikePatternValue
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.TxException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.general.TxAction
import org.vitrivr.cottontail.dbms.general.TxSnapshot
import org.vitrivr.cottontail.dbms.index.AbstractIndex
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.storage.serializers.ValueSerializerFactory
import java.nio.file.Path
import java.util.*

/**
 * Represents an [AbstractIndex] in the Cottontail DB data model, that uses a persistent [HashMap]
 * to map a [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Luca Rossetto & Ralph Gasser
 * @version 2.0.2
 */
class NonUniqueHashIndex(path: Path, parent: DefaultEntity) : AbstractIndex(path, parent) {
    /** Index-wide constants. */
    companion object {
        const val NUQ_INDEX_MAP = "cdb_nuq_tree_map"
    }

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.HASH

    /** True since [NonUniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [NonUniqueHashIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** The [NonUniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** Check if [Serializer] is compatible with this [NonUniqueHashIndex]. */
    private val serializer: GroupSerializer<Value> = ValueSerializerFactory.mapdb(this.columns[0].type).let {
        require(it is GroupSerializer<*>) { "NonUniqueHashIndex only supports value types with group serializers." }
        it as GroupSerializer<Value>
    }

    /** Map structure used for [NonUniqueHashIndex]. */
    private val map = this.store.treeMap(NUQ_INDEX_MAP, this.serializer, Serializer.LONG_ARRAY).createOrOpen()

    init {
        this.store.commit() /* Initial commit. */
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [NonUniqueHashIndex].
     *
     * [NonUniqueHashIndex] can be used to process EQUALS, IN AND LIKE comparison operations on the specified column
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean {
        if (predicate !is BooleanPredicate.Atomic) return false
        if (predicate.not) return false
        if (!predicate.columns.contains(this.columns[0])) return false
        return when (predicate.operator) {
            is ComparisonOperator.Binary.Equal,
            is ComparisonOperator.In -> true
            is ComparisonOperator.Binary.Like -> {
                val likeOp = predicate.operator as ComparisonOperator.Binary.Like
                likeOp.right.value is LikePatternValue.StartsWith
            }
            else -> false
        }
    }

    /**
     * Calculates the cost estimate of this [NonUniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is BooleanPredicate.Atomic || predicate.columns.first() != this.columns[0] || predicate.not -> Cost.INVALID
        predicate.operator is ComparisonOperator.Binary.Equal ||
        predicate.operator is ComparisonOperator.Binary.Like -> Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
        predicate.operator is ComparisonOperator.In -> {
            val inOp = predicate.operator as ComparisonOperator.In
            Cost.DISK_ACCESS_READ * inOp.right.size + Cost.MEMORY_ACCESS * inOp.right.size + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
        }
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context If the [TransactionContext] to create the [IndexTx] for..
     */
    override fun newTx(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): IndexTx = Tx(context)

    /**
     * An [IndexTx] that affects this [NonUniqueHashIndex].
     */
    private inner class Tx(context: org.vitrivr.cottontail.dbms.execution.TransactionContext) : AbstractIndex.Tx(context) {

        /** The default [TxSnapshot] of this [IndexTx]. Can be overriden! */
        override val snapshot = object : TxSnapshot {
            override val actions: List<TxAction> = emptyList()

            override fun commit() {
                for (c in this@Tx.mappingsCache) {
                    this@NonUniqueHashIndex.map.compute(c.key) { _, v ->
                        if (v == null) {
                            c.value.toLongArray()
                        } else {
                            v + c.value.toLongArray()
                        }
                    }
                }
                this@NonUniqueHashIndex.store.commit()
            }

            override fun rollback() {
                mappingsCache.clear()
                this@NonUniqueHashIndex.store.rollback()
            }

            override fun record(action: TxAction): Boolean = false
        }

        /** Internal cache that keeps Value to TupleId mappings in memory until commit. */
        private val mappingsCache = Object2ObjectOpenHashMap<Value, LinkedList<Long>>()

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to add a mapping for.
         * @param tupleId The [TupleId] for the mapping.
         */
        private fun addMapping(key: Value, tupleId: TupleId) {
            this.mappingsCache.compute(key) { _, v ->
                val l = v ?: LinkedList<TupleId>()
                l.add(tupleId)
                l
            }
        }

        /**
         * Removes a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to remove a mapping for.
         * @param tupleId The [TupleId] to remove.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun removeMapping(key: Value, tupleId: TupleId) {
            this@NonUniqueHashIndex.map.compute(key) { _, v ->
                v?.filter { it != tupleId }?.toLongArray() ?: v
            }
        }

        /**
         * Returns the number of entries in this [NonUniqueHashIndex.map] which DOES NOT directly
         * correspond to the number of [TupleId]s it encodes.
         *
         * @return Number of [TupleId]s in this [NonUniqueHashIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@NonUniqueHashIndex.map.size.toLong()
        }

        /**
         * (Re-)builds the [NonUniqueHashIndex].
         */
        override fun rebuild() = this.withWriteLock {
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Recreate entries. */
            this@NonUniqueHashIndex.map.clear()
            entityTx.scan(this.dbo.columns).forEach { record ->
                val value = record[this.dbo.columns[0]] ?: throw TxException.TxValidationException(
                    this.context.txId,
                    "A value cannot be null for instances of NonUniqueHashIndex ${this@NonUniqueHashIndex.name} but given value is (value = null, tupleId = ${record.tupleId})."
                )
                this.addMapping(value, record.tupleId)
            }
        }

        /**
         * Updates the [NonUniqueHashIndex] with the provided [Record]. This method determines, whether
         * the [Record] affected by the [Operation.DataManagementOperation] should be added or updated
         *
         * @param event [Operation.DataManagementOperation] to process.
         */
        override fun update(event: Operation.DataManagementOperation) = this.withWriteLock {
            when (event) {
                is Operation.DataManagementOperation.InsertOperation -> {
                    val value = event.inserts[this.dbo.columns[0]]
                    if (value != null) {
                        this.addMapping(value, event.tupleId)
                    }
                }
                is Operation.DataManagementOperation.UpdateOperation -> {
                    val old = event.updates[this.dbo.columns[0]]?.first
                    if (old != null) {
                        this.removeMapping(old, event.tupleId)
                    }
                    val new = event.updates[this.dbo.columns[0]]?.second
                    if (new != null) {
                        this.addMapping(new, event.tupleId)
                    }
                }
                is Operation.DataManagementOperation.DeleteOperation -> {
                    val old = event.deleted[this.dbo.columns[0]]
                    if (old != null) {
                        this.removeMapping(old, event.tupleId)
                    }
                }
            }
        }

        /**
         * Clears the [NonUniqueHashIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.withWriteLock {
            this@NonUniqueHashIndex.dirtyField.compareAndSet(false, true)
            this@NonUniqueHashIndex.map.clear()
        }

        /**
         * Performs a lookup through this [NonUniqueHashIndex.Tx] and returns a [Iterator] of
         * all [Record]s that match the [Predicate]. Only supports [ BooleanPredicate.AtomicBooleanPredicate]s.
         *
         * The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Iterator<Record> {

            /** Local [ BooleanPredicate.AtomicBooleanPredicate] instance. */
            private val predicate: BooleanPredicate.Atomic

            /** Pre-fetched entries that match the [Predicate]. */
            private val elements = LinkedList<Pair<TupleId, Value>>()

            init {
                require(predicate is BooleanPredicate.Atomic) { "NonUniqueHashIndex.filter() does only support Atomic.Literal boolean predicates." }
                require(!predicate.not) { "NonUniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                this.predicate = predicate
                this@Tx.withReadLock {
                    when (this.predicate.operator) {
                        is ComparisonOperator.In -> {
                            val inOp = this.predicate.operator as ComparisonOperator.In
                            inOp.right.forEach { v ->
                                val value = v.value
                                if (value != null) {
                                    val subset = this@NonUniqueHashIndex.map[value]
                                    subset?.forEach { this.elements.add(Pair(it, value)) }
                                }
                            }
                        }
                        is ComparisonOperator.Binary.Equal -> {
                            val eqOp = this.predicate.operator as ComparisonOperator.Binary.Equal
                            val value = eqOp.right.value
                            if (value != null) {
                                val subset = this@NonUniqueHashIndex.map[value]
                                subset?.forEach { this.elements.add(Pair(it, value)) }
                            }
                        }
                        is ComparisonOperator.Binary.Like -> {
                            val likeOp = this.predicate.operator as ComparisonOperator.Binary.Like
                            val operand = likeOp.right.value
                            if (operand is LikePatternValue.StartsWith) {
                                val prefix = this@NonUniqueHashIndex.map.prefixSubMap(StringValue(operand.startsWith.toString()))
                                for ((k, v) in prefix) {
                                    for (l in v) {
                                        this.elements.add(Pair(l, k))
                                    }
                                }
                            } else {
                                throw IllegalArgumentException("NonUniqueHashIndex.filter() does only support LIKE operators with prefix matching (i.e. LIKE XYZ%).")
                            }
                        }
                        else -> throw IllegalArgumentException("NonUniqueHashIndex.filter() does only support EQUAL, IN and LIKE operators.")
                    }
                }
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                return this.elements.isNotEmpty()
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                val next = this.elements.poll()
                return StandaloneRecord(next.first, this@Tx.dbo.columns, arrayOf(next.second))
            }
        }

        /**
         * The [NonUniqueHashIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Iterator].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Iterator<Record> {
            throw UnsupportedOperationException("The NonUniqueHashIndex does not support ranged filtering!")
        }
    }
}
