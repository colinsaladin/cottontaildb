package org.vitrivr.cottontail.database.index.basics

import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.basics.AbstractIndex.Tx
import org.vitrivr.cottontail.database.index.pq.PQIndex
import org.vitrivr.cottontail.database.logging.operations.Operation
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException


/**
 * An abstract [Index] implementation that outlines the fundamental structure of a high-dimensional (HD).
 *
 * Implementations of [Index]es in Cottontail DB should inherit from this class.
 *
 * @see Index
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
abstract class AbstractHDIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /** The dimensionality of this [AbstractHDIndex]. */
    val dimension: Int
        get() = this.columns[0].type.logicalSize

    /**
     * A [Tx] that affects this [AbstractIndex].
     */
    protected abstract inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context), IndexTx {

    }
}