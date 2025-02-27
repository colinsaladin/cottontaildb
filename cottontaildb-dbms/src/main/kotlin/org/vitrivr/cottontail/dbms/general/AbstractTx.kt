package org.vitrivr.cottontail.dbms.general

import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import java.util.concurrent.locks.ReentrantLock

/**
 * An abstract [Tx] implementation that provides some basic functionality.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
abstract class AbstractTx(final override val context: TransactionContext) : Tx {
    /**
     * This is a [ReentrantLock] that makes sure that only one thread at a time can access this [AbstractTx] instance.
     *
     * While access by different [TransactionContext]s is handled by the respective lock manager, it is still possible that
     * different threads with the same [TransactionContext] try to access this [Tx], e.g., for intra query parallelism.
     * This requires synchronisation.
     */
    val txLatch: ReentrantLock = ReentrantLock()

    /**
     * Called when the global transaction is committed.
     *
     * This implementation only calls the [cleanup] method, which can be implemented by subclasses.
     */
    override fun beforeCommit() {
        this.cleanup()
    }

    /**
     * Called when the global transaction is rolled back.
     *
     * This implementation only calls the [cleanup] method, which can be implemented by subclasses.
     */
    override fun beforeRollback() {
        this.cleanup()
    }

    /**
     * Used to perform cleanup-operations necessary after a transaction concludes.
     */
    abstract fun cleanup()
}