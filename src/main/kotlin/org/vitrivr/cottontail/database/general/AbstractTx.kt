package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.database.index.basics.AbstractIndex
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.exceptions.TxException
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * An abstract [Tx] implementation that provides some basic functionality.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
abstract class AbstractTx(final override val context: TransactionContext) : Tx {
    /** Flag indicating whether this [AbstractTx] was closed */
    @Volatile
    final override var status: TxStatus = TxStatus.CLEAN
        protected set

    /**
     * This is a [ReentrantReadWriteLock] that makes sure that only one thread at a time can access this [AbstractTx] instance.
     *
     * While access by different [TransactionContext]s is handled by the respective lock manager,
     * it is still possible that different threads with the same [TransactionContext] try to access
     * this [Tx]. This needs synchronisation.
     */
    val txLatch: ReentrantReadWriteLock = ReentrantReadWriteLock()

    /**
     * Commits all changes made through this [AbstractTx] and releases all locks obtained.
     *
     * This implementation only makes structural changes to the [AbstractTx] (updates status,
     * sanity checks etc). Implementing classes need to implement [TxSnapshot] to execute the
     * actual commit.
     */
    override fun commit() {

    }

    /**
     * Makes a rollback on all made through this [AbstractTx] and releases all locks obtained.
     *
     * This implementation only makes structural changes to the [AbstractTx] (updates status,
     * sanity checks etc). Implementing classes need to implement [TxSnapshot] to execute the actual
     * commit.
     */
    override fun rollback() {

    }

    /**
     * Checks if this [AbstractIndex.Tx] is in a valid state for write operations to happen and sets its
     * [status] to [TxStatus.DIRTY]
     */
    protected inline fun <T> withWriteLock(block: () -> (T)): T {
        if (this.status == TxStatus.CLOSED) throw TxException.TxClosedException(this.context.txId)
        if (this.status == TxStatus.ERROR) throw TxException.TxInErrorException(this.context.txId)
        this.context.requestLock(this.dbo, LockMode.EXCLUSIVE)
        this.status = TxStatus.DIRTY
        return this.txLatch.write {
            block()
        }
    }

    /**
     * Checks if this [AbstractIndex.Tx] is in a valid state for read operations to happen.
     */
    protected inline fun <T> withReadLock(block: () -> (T)): T {
        if (this.status == TxStatus.CLOSED) throw TxException.TxClosedException(this.context.txId)
        if (this.status == TxStatus.ERROR) throw TxException.TxInErrorException(this.context.txId)
        this.context.requestLock(this.dbo, LockMode.SHARED)
        return this.txLatch.read {
            block()
        }
    }
}