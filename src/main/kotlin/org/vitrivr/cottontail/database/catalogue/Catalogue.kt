package org.vitrivr.cottontail.database.catalogue

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.functions.FunctionRegistry
import org.vitrivr.cottontail.model.basics.Name
import java.nio.file.Path

/**
 * The main catalogue in Cottontail DB. It contains references to all the [Schema]s managed by
 * Cottontail DB and is the main way of accessing these [Schema]s and creating new ones.
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Catalogue : DBO, AutoCloseable {
    /** Reference to [Config] object. */
    val config: Config

    /** The [FunctionRegistry] exposed by this [Catalogue]. */
    val functions: FunctionRegistry

    /** Root to Cottontail DB root folder. */
    val path: Path

    /** Constant name of the [Catalogue] object. */
    override val name: Name.RootName

    /** Constant parent [DBO], which is null in case of the [Catalogue]. */
    override val parent: DBO?

    /** Status indicating whether this [Catalogue] is open or closed. */
    override val closed: Boolean

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