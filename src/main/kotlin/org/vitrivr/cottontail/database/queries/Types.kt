package org.vitrivr.cottontail.database.queries

import org.vitrivr.cottontail.database.column.ColumnDef

/** A [GroupId] is an identifier to identify sub-trees in an query execution plan. */
typealias GroupId = Int

/** A [Digest] is a hash value that uniquely identifies a tree of nodes. The same combination of nodes should lead to the same [Digest]. */
typealias Digest = Long

/** A [ColumnPair] is a [Pair] of logical to physical [ColumnDef]. */
typealias ColumnPair = Pair<ColumnDef<*>, ColumnDef<*>?>

/** Returns the logical [ColumnDef] of this [ColumnPair]. */
fun ColumnPair.logical() = this.first

/** Returns the physical [ColumnDef] of this [ColumnPair]. */
fun ColumnPair.physical() = this.second