package org.vitrivr.cottontail.dbms.general

enum class TxStatus {
    CLEAN
    /** Transaction has not been used to modify anything yet. */
    ,
    DIRTY
    /** Transaction has uncommitted changes. */
    ,
    ERROR
    /** Transaction is in an error state, since some modifications failed. */
    ,
    CLOSED
    /** Transaction was closed. */
}