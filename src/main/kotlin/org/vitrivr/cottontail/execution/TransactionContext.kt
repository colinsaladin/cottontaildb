package org.vitrivr.cottontail.execution

import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.locking.Lock
import org.vitrivr.cottontail.database.locking.LockManager
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.operations.Operation
import org.vitrivr.cottontail.model.basics.TransactionId

/**
 * A [TransactionContext] used by operators and their sub transactions to execute and obtain necessary locks on database objects.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
interface TransactionContext {

    /** The [TransactionId] of this [TransactionContext]. */
    val txId: TransactionId

    /** The Xodus [Transaction] associated with this [TransactionContext]. */
    val xodusTx: Transaction

    /** The [TransactionType] of this [TransactionContext]. */
    val type: TransactionType

    /** The [TransactionStatus] of this [TransactionContext]. */
    val state: TransactionStatus

    /** Flag indicating if this [TransactionContext] is readonly. */
    val readonly: Boolean

    /**
     * Obtains a [Tx] for the given [DBO]. This method should make sure, that only one [Tx] per [DBO] is created.
     *
     * @param dbo The DBO] to create the [Tx] for.
     * @return The resulting [Tx]
     */
    fun getTx(dbo: DBO): Tx

    /**
     * Acquires a [Lock] on a [DBO] for the given [LockMode]. This call is delegated to the
     * [LockManager] and really just a convenient way for [Tx] objects to obtain locks.
     *
     * @param dbo [DBO] The [DBO] to request the lock for.
     * @param mode The desired [LockMode]
     */
    fun requestLock(dbo: DBO, mode: LockMode)

    /**
     * Signals a [Operation.DataManagementOperation] to this [TransactionContext].
     *
     * Implementing methods must process these [Operation.DataManagementOperation]s quickly, since they are usually
     * triggered during an ongoing transaction.
     *
     * @param action The [Operation.DataManagementOperation] that has been reported.
     */
    fun signalEvent(action: Operation.DataManagementOperation)
}