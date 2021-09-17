package org.vitrivr.cottontail.database.general

import org.vitrivr.cottontail.database.index.basics.AbstractIndex
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.exceptions.TxException
import java.util.concurrent.locks.ReentrantLock
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
     * This is a [ReentrantLock] that makes sure that only one thread at a time can access this [AbstractTx] instance.
     *
     * While access by different [TransactionContext]s is handled by the respective lock manager, it is still possible that
     * different threads with the same [TransactionContext] try to access this [Tx]. This needs synchronisation.
     */
    val txLatch: ReentrantLock = ReentrantLock()

    /**
     *  Called when the global transaction is committed.
     *
     * This implementation only calls the [cleanup] method, which can be implemented by subclasses.
     */
    override fun onCommit() {
        this.cleanup()
    }

    /**
     * Called when the global transaction is rolled back.
     *
     * This implementation only calls the [cleanup] method, which can be implemented by subclasses.
     */
    override fun onRollback() {
        this.cleanup()
    }

    /**
     * Used to perform cleanup-operations necessary after a transaction concludes.
     */
    abstract fun cleanup()
}