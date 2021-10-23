package org.vitrivr.cottontail.database.index.hash

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.database.catalogue.storeName
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.AbstractIndex
import org.vitrivr.cottontail.database.index.basics.IndexConfig
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.database.index.basics.NoIndexConfig
import org.vitrivr.cottontail.database.operations.Operation
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding
import java.util.*
import kotlin.concurrent.withLock

/**
 * Represents an index in the Cottontail DB data model, that uses a persistent [HashMap] to map a
 * unique [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class UniqueHashIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.BTREE_UQ

    /** The [UniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** True since [UniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [UniqueHashIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** [UniqueHashIndex] does not have an [IndexConfig]*/
    override val config: IndexConfig = NoIndexConfig

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [UniqueHashIndex]. [UniqueHashIndex] can be used to process IN and EQUALS
     * comparison operations on the specified column
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate.Atomic
            && !predicate.not
            && predicate.columns.contains(this.columns[0])
            && (predicate.operator is ComparisonOperator.In || predicate.operator is ComparisonOperator.Binary.Equal)

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is BooleanPredicate.Atomic || predicate.columns.first() != this.columns[0] || predicate.not -> Cost.INVALID
        predicate.operator is ComparisonOperator.Binary.Equal -> Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS, predicate.columns.sumOf { it.type.physicalSize }.toFloat())
        predicate.operator is ComparisonOperator.In -> Cost(Cost.COST_DISK_ACCESS_READ * predicate.operator.right.size, Cost.COST_MEMORY_ACCESS * predicate.operator.right.size, predicate.columns.sumOf { it.type.physicalSize }.toFloat())
        else -> Cost.INVALID
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context [TransactionContext] to open the [AbstractIndex.Tx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * An [IndexTx] that affects this [UniqueHashIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The internal [XodusBinding] reference used for de-/serialization. */
        private val binding: XodusBinding<*> = this@UniqueHashIndex.columns[0].type.serializerFactory().xodus(this@UniqueHashIndex.columns[0].type.logicalSize, this@UniqueHashIndex.columns[0].nullable)

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to add a mapping for.
         * @param tupleId The [TupleId] for the mapping.
         *
         * This is an internal function and can be used safely with values o
         */
        private fun addMapping(key: Value, tupleId: TupleId): Boolean {
            val keyRaw = (this.binding as XodusBinding<Value>).valueToEntry(key)
            val tupleIdRaw = LongBinding.longToCompressedEntry(tupleId)
            return if (this.dataStore.get(this.context.xodusTx, keyRaw) != null) {
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
        private fun removeMapping(key: Value): Boolean {
            val keyRaw = (this.binding as XodusBinding<Value>).valueToEntry(key)
            return this.dataStore.delete(this.context.xodusTx, keyRaw)
        }

        /**
         * (Re-)builds the [UniqueHashIndex].
         */
        override fun rebuild() = this.txLatch.withLock {
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Truncate, reopen and repopulate store. */
            this.clear()
            entityTx.cursor(this@UniqueHashIndex.columns).forEach { record ->
                val value = record[this.dbo.columns[0]] ?: throw TxException.TxValidationException(this.context.txId, "Value cannot be null for UniqueHashIndex ${this@UniqueHashIndex.name} given value is (value = null, tupleId = ${record.tupleId}).")
                if (!this.addMapping(value, record.tupleId)) {
                    throw TxException.TxValidationException(this.context.txId, "Value must be unique for UniqueHashIndex ${this@UniqueHashIndex.name} but is not (value = $value, tupleId = ${record.tupleId}).")
                }
            }
        }

        /**
         * Clears the [UniqueHashIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            this@UniqueHashIndex.parent.parent.parent.environment.truncateStore(this@UniqueHashIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@UniqueHashIndex.parent.parent.parent.environment.openStore(
                this@UniqueHashIndex.name.storeName(),
                StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING,
                this.context.xodusTx,
                false
            ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@UniqueHashIndex.name} is missing.")
        }

        /**
         * Updates the [UniqueHashIndex] with the provided [Operation.DataManagementOperation]s. This method determines,
         * whether the [Record] affected by the [Operation.DataManagementOperation] should be added or updated
         *
         * @param event [Operation.DataManagementOperation]s to process.
         */
        override fun update(event: Operation.DataManagementOperation) = this.txLatch.withLock {
            when (event) {
                is Operation.DataManagementOperation.InsertOperation-> {
                    val value = event.inserts[this.dbo.columns[0]]
                    if (value != null) {
                        this.addMapping(value, event.tupleId)
                    }
                }
                is Operation.DataManagementOperation.UpdateOperation -> {
                    val old = event.updates[this.dbo.columns[0]]?.first
                    if (old != null) {
                        this.removeMapping(old)
                    }
                    val new = event.updates[this.dbo.columns[0]]?.second
                    if (new != null) {
                        this.addMapping(new, event.tupleId)
                    }
                }
                is Operation.DataManagementOperation.DeleteOperation -> {
                    val old = event.deleted[this.dbo.columns[0]]
                    if (old != null) {
                        this.removeMapping(old)
                    }
                }
            }
        }

        /**
         * Performs a lookup through this [UniqueHashIndex.Tx] and returns a [Iterator] of
         * all [Record]s that match the [Predicate]. Only supports [BooleanPredicate.Atomic]s.
         *
         * The [Iterator] is not thread safe!
         **
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = object : Iterator<Record> {

            /** Local [BooleanPredicate.Atomic] instance. */
            private val predicate: BooleanPredicate.Atomic

            /** A [Queue] with values that should be queried. */
            private val queryValueQueue: Queue<Value> = LinkedList()

            /** The current query [Value]. */
            private var queryValue: Value

            /** Internal cursor used for navigation. */
            private var cursor: Cursor

            /* Perform initial sanity checks. */
            init {
                require(predicate is BooleanPredicate.Atomic) { "UniqueHashIndex.filter() does only support Atomic.Literal boolean predicates." }
                require(!predicate.not) { "UniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                this.predicate = predicate
                when (predicate.operator) {
                    is ComparisonOperator.In -> this.queryValueQueue.addAll(predicate.operator.right.mapNotNull { it.value })
                    is ComparisonOperator.Binary.Equal -> this.queryValueQueue.add(predicate.operator.right.value ?: throw IllegalArgumentException("UniqueHashIndex.filter() does not support NULL operands."))
                    else -> throw IllegalArgumentException("UniqueHashIndex.filter() does only support EQUAL and IN operators.")
                }

                /** Initialize cursor. */
                this.cursor = this@Tx.dataStore.openCursor(this@Tx.context.xodusTx)
                this.queryValue = this.queryValueQueue.poll() ?: throw IllegalArgumentException("UniqueHashIndex.filter() does not support NULL operands.")
                this.cursor.getSearchKey(StringBinding.BINDING.objectToEntry(this.queryValue))
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean = this.nextQueryValue()

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): Record {
                val value = this@Tx.binding.entryToValue(this.cursor.value)
                val tid = LongBinding.compressedEntryToLong(this.cursor.key)
                return StandaloneRecord(tid, this@UniqueHashIndex.produces, arrayOf(value))
            }

            /**
             * Moves to next query value
             */
            private fun nextQueryValue(): Boolean {
                this.queryValue = this.queryValueQueue.poll() ?: return false
                return this.cursor.getSearchKey(StringBinding.BINDING.objectToEntry(this.queryValue)) != null
            }
        }

        /**
         * The [UniqueHashIndex] does not support ranged filtering!
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
