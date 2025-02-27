package org.vitrivr.cottontail.dbms.catalogue

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * The main catalogue in Cottontail DB. It contains references to all the [Schema]s managed by
 * Cottontail DB and is the main way of accessing these [Schema]s and creating new ones.
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Catalogue : DBO {
    /** Reference to [Config] object. */
    val config: Config

    /** The [FunctionRegistry] exposed by this [Catalogue]. */
    val functions: FunctionRegistry

    /** Constant name of the [Catalogue] object. */
    override val name: Name.RootName

    /** Constant parent [DBO], which is null in case of the [Catalogue]. */
    override val parent: DBO?

    /**
     * Creates and returns a new [CatalogueTx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [CatalogueTx] for.
     * @return New [CatalogueTx]
     */
    override fun newTx(context: TransactionContext): CatalogueTx

    /**
     * Closes the [Catalogue] and all objects contained within.
     */
    override fun close()
}