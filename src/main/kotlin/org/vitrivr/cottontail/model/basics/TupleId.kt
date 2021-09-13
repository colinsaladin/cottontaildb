package org.vitrivr.cottontail.model.basics

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.LongBinding

/** Type alias for [TupleId]; a [TupleId] is a positive [Long] value (negative [TupleId]s are invalid).*/
typealias TupleId = Long

/** */
typealias TransactionId = Long


/**
 * Converts [TupleId] to an [ArrayByteIterable] used for persistence through Xodus.
 */
fun TupleId.toKey(): ArrayByteIterable = LongBinding.longToCompressedEntry(this)
