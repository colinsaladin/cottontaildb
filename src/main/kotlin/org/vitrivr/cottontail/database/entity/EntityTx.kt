package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.model.basics.*

/**
 * A [Tx] that operates on a single [Entity]. [Tx]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface EntityTx : Tx, Scanable, Countable, Modifiable {

    /** Reference to the [Entity] this [EntityTx] belongs to. */
    override val dbo: Entity

    /**
     * Reads the specified [TupleId] and the specified [ColumnDef] through this [EntityTx].
     *
     * @param tupleId The [TupleId] to read.
     * @param columns The [ColumnDef] to read.
     */
    fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record

    /**
     * Returns the maximum [TupleId] known by this [EntityTx].
     *
     * @return Maximum [TupleId] known by this [EntityTx]
     */
    fun maxTupleId(): TupleId

    /**
     * Lists all [ColumnDef]s for the [Entity] associated with this [EntityTx].
     *
     * @return List of all [ColumnDef]s.
     */
    fun listColumns(): List<ColumnDef<*>>

    /**
     * Returns the [Column] for the specified [Name.ColumnName]. Should be able to handle
     * both simple names as well as fully qualified names.
     *
     * @param name The [Name.ColumnName] of the [Column].
     * @return [Column].
     */
    fun columnForName(name: Name.ColumnName): Column<*>

    /**
     * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
     *
     * @return List of [Name.IndexName] managed by this [EntityTx]
     */
    fun listIndexes(): List<Index>

    /**
     * Returns the [IndexTx] for the given [Name.IndexName].
     *
     * @param name The [Name.IndexName] of the [Index] the [IndexTx] belongs to.
     * @return [IndexTx]
     */
    fun indexForName(name: Name.IndexName): Index

    /**
     * Creates the [Index] with the given settings
     *
     * @param name [Name.IndexName] of the [Index] to create.
     * @param type Type of the [Index] to create.
     * @param columns The list of [Name.ColumnName] to create [Index] for.
     * @param params Additional parameters for index creation.
     * @return Newly created [Index] for use in context of this [Tx]
     */
    fun createIndex(name: Name.IndexName, type: IndexType, columns: Array<Name.ColumnName>, params: Map<String, String>): Index

    /**
     * Drops the [Index] with the given name.
     *
     * @param name [Name.IndexName] of the [Index] to drop.
     */
    fun dropIndex(name: Name.IndexName)

    /**
     * Optimizes the [Entity] underlying this [EntityTx]. Optimization involves rebuilding of [Index]es and statistics.
     */
    fun optimize()
}