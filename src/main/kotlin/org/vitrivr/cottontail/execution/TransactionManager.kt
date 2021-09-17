package org.vitrivr.cottontail.execution

import it.unimi.dsi.fastutil.Hash.VERY_FAST_LOAD_FACTOR
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.database.locking.Lock
import org.vitrivr.cottontail.database.locking.LockHolder
import org.vitrivr.cottontail.database.locking.LockManager
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.execution.TransactionManager.Transaction
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.TransactionId
import java.util.*
import java.util.concurrent.atomic.AtomicLong

/**
 * The default [TransactionManager] for Cottontail DB. It hosts all the necessary facilities to
 * create and execute queries within different [Transaction]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class TransactionManager(private val catalogue: DefaultCatalogue, private val transactionHistorySize: Int, transactionTableSize: Int) {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(TransactionManager::class.java)
    }

    /** Map of [Transaction]s that are currently PENDING or RUNNING. */
    private val transactions = Collections.synchronizedMap(Long2ObjectOpenHashMap<Transaction>(transactionTableSize, VERY_FAST_LOAD_FACTOR))

    /** Internal counter to generate [TransactionId]s. */
    private val tidCounter = AtomicLong()

    /** The [LockManager] instance used by this [TransactionManager]. */
    val lockManager = LockManager<DBO>()

    /** List of ongoing or past transactions (limited to [transactionHistorySize] entries). */
    val transactionHistory = Collections.synchronizedList(LinkedList<Transaction>())

    /**
     * Returns the [Transaction] for the provided [TransactionId].
     *
     * @param txId [TransactionId] to return the [Transaction] for.
     * @return [Transaction] or null
     */
    operator fun get(txId: TransactionId): Transaction? = this.transactions[txId]

    /**
     * A concrete [Transaction] used for executing a query.
     *
     * A [Transaction] can be of different [TransactionType]s. Their execution semantics may differ slightly.
     *
     * @author Ralph Gasser
     * @version 1.3.0
     */
    inner class Transaction(override val type: TransactionType) : LockHolder<DBO>(this@TransactionManager.tidCounter.getAndIncrement()), TransactionContext {

        /** The [TransactionStatus] of this [Transaction]. */
        @Volatile
        override var state: TransactionStatus = TransactionStatus.READY
            private set

        /** The [jetbrains.exodus.env.Transaction] used by this [Transaction]. */
        override val xodusTx: jetbrains.exodus.env.Transaction
            get() = this@TransactionManager.catalogue.environment.beginTransaction()

        /** Map of all [Tx] that have been created as part of this [Transaction]. */
        private val txns: MutableMap<DBO, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /** A [Mutex] data structure used for synchronisation on the [Transaction]. */
        private val mutex = Mutex()

        /** Number of [Tx] held by this [Transaction]. */
        val numberOfTxs: Int
            get() = this.txns.size

        /** Timestamp of when this [Transaction] was created. */
        val created = System.currentTimeMillis()

        /** Timestamp of when this [Transaction] was either COMMITTED or ABORTED. */
        var ended: Long? = null
            private set

        init {
            /** Add this to transaction history and transaction table. */
            this@TransactionManager.transactions[this.txId] = this
            this@TransactionManager.transactionHistory.add(this)
            if (this@TransactionManager.transactionHistory.size >= this@TransactionManager.transactionHistorySize) {
                this@TransactionManager.transactionHistory.removeAt(0)
            }
        }

        /**
         * Returns the [Tx] for the provided [DBO]. Creating [Tx] through this method makes sure,
         * that only on [Tx] per [DBO] and [Transaction] is created.
         *
         * @param dbo [DBO] to return the [Tx] for.
         * @return entity [Tx]
         */
        override fun getTx(dbo: DBO): Tx {
            check(this.state === TransactionStatus.READY || this.state === TransactionStatus.RUNNING) {
                "Cannot obtain Tx for DBO '${dbo.name}' for ${this.txId} because it is in wrong state (s = ${this.state})."
            }
            return this.txns.computeIfAbsent(dbo) { dbo.newTx(this) }
        }

        /**
         * Tries to acquire a [Lock] on a [DBO] for the given [LockMode]. This call is delegated to the
         * [LockManager] and really just a convenient way for [Tx] objects to obtain locks.
         *
         * @param dbo [DBO] The [DBO] to request the lock for.
         * @param mode The desired [LockMode]
         */
        override fun requestLock(dbo: DBO, mode: LockMode) {
            check(this.state === TransactionStatus.READY || this.state === TransactionStatus.RUNNING) {
                "Cannot obtain lock on DBO '${dbo.name}' for ${this.txId} because it is in wrong state (s = ${this.state})."
            }
            this@TransactionManager.lockManager.lock(this, dbo, mode)
        }

        /**
         * Signals a [Operation.DataManagementOperation] to this [Transaction].
         *
         * Implementing methods must process these [Operation.DataManagementOperation]s quickly, since they are usually
         * triggered during an ongoing transaction.
         *
         * @param action The [Operation.DataManagementOperation] that has been reported.
         */
        override fun signalEvent(action: Operation.DataManagementOperation) {
            /* ToDo: Do something with the events. */
        }

        /**
         * Schedules an [Operator] for execution in this [Transaction] and blocks, until execution has completed.
         *
         * @param context The [QueryContext] to execute the [Operator] in.
         * @param operator The [Operator.SinkOperator] that should be executed.
         */
        override fun execute(operator: Operator, context: QueryContext): Flow<Record> = operator.toFlow(context).onStart {
            this@Transaction.mutex.lock()
            check(this@Transaction.state === TransactionStatus.READY) { "Cannot start execution of transaction ${this@Transaction.txId} because it is in the wrong state (s = ${this@Transaction.state})." }
            this@Transaction.state = TransactionStatus.RUNNING
        }.onEach {
            if (this.state === TransactionStatus.KILLED) {
                throw CancellationException("Transaction $this.txId was killed by the user.")
            }
        }.onCompletion {
            if (it == null) {
                this@Transaction.state = TransactionStatus.READY
                if (this@Transaction.type === TransactionType.USER_IMPLICIT) {
                    this@Transaction.commitInternal()
                }
            } else {
                this@Transaction.state = TransactionStatus.ERROR
                if (this@Transaction.type === TransactionType.USER_IMPLICIT) {
                    this@Transaction.rollbackInternal()
                }
            }
            this@Transaction.mutex.unlock()
        }

        /**
         * Commits this [Transaction] thus finalizing and persisting all operations executed so far.
         */
        fun commit() = runBlocking {
            this@Transaction.mutex.withLock {
                this@Transaction.commitInternal()
            }
        }

        /**
         * Internal commit logic: Commits this [Transaction] thus finalizing and persisting all operations executed so far.
         *
         * When calling this method, a lock on [mutex] should be held!
         */
        private fun commitInternal() {
            check(this@Transaction.state === TransactionStatus.READY) { "Cannot commit transaction ${this@Transaction.txId} because it is in wrong state (s = ${this@Transaction.state})." }
            check(this@Transaction.txns.values.none { it.status == TxStatus.ERROR }) { "Cannot commit transaction ${this@Transaction.txId} because some of the participating Tx are in an error state." }
            this@Transaction.state = TransactionStatus.FINALIZING
            try {
                this@Transaction.txns.values.reversed().forEachIndexed { i, txn ->
                    try {
                        txn.onCommit()
                    } catch (e: Throwable) {
                        LOGGER.error("An error occurred while committing Tx $i (${txn.dbo.name}) of transaction ${this@Transaction.txId}. This is serious!", e)
                    }
                }
            } finally {
                this@Transaction.finalize(true)
            }
        }


        /**
         * Rolls back this [Transaction] thus reverting all operations executed so far.
         */
        fun rollback() = runBlocking {
            this@Transaction.mutex.withLock {
                this@Transaction.rollbackInternal()
            }
        }

        /**
         * Internal rollback logic: Rolls back this [Transaction] thus reverting all operations executed so far.
         *
         * When calling this method, a lock on [mutex] should be held!
         */
        private fun rollbackInternal() {
            check(this@Transaction.state === TransactionStatus.READY || this@Transaction.state === TransactionStatus.ERROR || this@Transaction.state === TransactionStatus.KILLED) { "Cannot rollback transaction ${this@Transaction.txId} because it is in wrong state (s = ${this@Transaction.state})." }
            this@Transaction.state = TransactionStatus.FINALIZING
            try {
                this@Transaction.txns.values.reversed().forEachIndexed { i, txn ->
                    try {
                        txn.onRollback()
                    } catch (e: Throwable) {
                        LOGGER.error("An error occurred while rolling back Tx $i (${txn.dbo.name}) of transaction ${this@Transaction.txId}. This is serious!", e)
                    }
                }
            } finally {
                this@Transaction.finalize(false)
            }
        }

        /**
         * Tries to kill this [Transaction] interrupting all running queries.
         *
         * A call to this method is a best-effort attempt to stop all ongoing queries. After a transaction was killed
         * successfully, all changes are rolled back.
         */
        fun kill() = runBlocking {
            check(this@Transaction.state === TransactionStatus.READY || this@Transaction.state === TransactionStatus.RUNNING) { "Cannot kill transaction ${this@Transaction.txId} because it is in wrong state (s = ${this@Transaction.state})." }
            this@Transaction.state = TransactionStatus.KILLED
            this@Transaction.rollback()
        }

        /**
         * Finalizes the state of this [Transaction].
         *
         * @param committed True if [Transaction] was committed, false otherwise.
         */
        private fun finalize(committed: Boolean) {
            this@Transaction.allLocks().forEach { this@TransactionManager.lockManager.unlock(this@Transaction, it.obj) }
            this@Transaction.txns.clear()
            this@Transaction.ended = System.currentTimeMillis()
            this@Transaction.state = if (committed) {
                TransactionStatus.COMMIT
            } else {
                TransactionStatus.ROLLBACK
            }
            this@TransactionManager.transactions.remove(this@Transaction.txId)
        }
    }
}