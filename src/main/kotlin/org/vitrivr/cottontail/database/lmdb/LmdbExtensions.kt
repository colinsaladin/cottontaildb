package org.vitrivr.cottontail.database.lmdb

import org.lmdbjava.Env
import org.lmdbjava.Txn
import java.nio.ByteBuffer

/**
 *
 */
fun <T> Env<ByteBuffer>.executeInWriteTransaction(action: (Txn<ByteBuffer>) -> T): T {
    val txn = this.txnWrite()
    try {
        val ret = action(txn)
        txn.commit()
        return ret
    } catch (e: Throwable) {
        txn.abort()
        throw e
    } finally {
        txn.close()
    }
}