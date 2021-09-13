package org.vitrivr.cottontail.database.index.basics

import jetbrains.exodus.env.Store
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.catalogue.storeName
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * An abstract [Index] implementation that outlines the fundamental structure. Implementations of
 * [Index]es in Cottontail DB should inherit from this class.
 *
 * @see Index
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
abstract class AbstractIndex(final override val name: Name.IndexName, final override val parent: DefaultEntity) : Index {

    /** A [DefaultSchema] belongs to its parent [DefaultCatalogue]. */
    final override val catalogue: DefaultCatalogue = this.parent.catalogue

    /** The [ColumnDef] that are covered (i.e. indexed) by this [AbstractIndex]. */
    final override val columns: Array<ColumnDef<*>>
        get() = this.catalogue.environment.computeInTransaction { tx ->
            DefaultCatalogue.readEntryForIndex(this.name, this.catalogue, tx).columns.map {
                DefaultCatalogue.readEntryForColumn(it, this.catalogue, tx).toColumnDef()
            }.toTypedArray()
        }

    /**
     * Flag indicating, whether this [AbstractIndex] reflects all changes done to the [DefaultEntity] it belongs to.
     *
     * This is a snapshot and may change immediately!
     */
    final override val state: IndexState
        get() = this.catalogue.environment.computeInTransaction { tx ->
            DefaultCatalogue.readEntryForIndex(this.name, this.parent.parent.parent, tx).state
        }

    /**
     * Number of entries in this [AbstractIndex].
     *
     * This is a snapshot and may change immediately.
     */
    final override val count: Long
        get() = this.catalogue.environment.computeInTransaction { tx ->
            this.catalogue.environment.openStore(this.name.storeName(), this.type.storeConfig(), tx).count(tx)
        }

    /** The order in which results of this [Index] appear. Defaults to an empty array, which indicates no particular order. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** The [DBOVersion] of this [AbstractIndex]. */
    override val version: DBOVersion = DBOVersion.V3_0

    /** Flag indicating if this [AbstractIndex] has been closed. */
    override val closed: Boolean
        get() = this.parent.closed

    /**
     * A [Tx] that affects this [AbstractIndex].
     */
    protected abstract inner class Tx(context: TransactionContext) : AbstractTx(context), IndexTx {

        /** Internal data [Store] reference. */
        protected var dataStore: Store = this@AbstractIndex.catalogue.environment.openStore(this@AbstractIndex.name.storeName(), this@AbstractIndex.type.storeConfig(), this.context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@AbstractIndex.name} (${this@AbstractIndex.type}) is missing.")

        /** Reference to the [AbstractIndex] */
        override val dbo: AbstractIndex
            get() = this@AbstractIndex

        /** The order in which results of this [IndexTx] appear. Empty array that there is no particular order. */
        override val order: Array<Pair<ColumnDef<*>, SortOrder>>
            get() = this@AbstractIndex.order

        /** The [IndexType] of the [AbstractIndex] that underpins this [IndexTx]. */
        override val type: IndexType
            get() = this@AbstractIndex.type

        /** Flag indicating, if this [AbstractIndex] reflects all changes done to the [DefaultEntity]it belongs to. */
        override val state: IndexState
            get() = DefaultCatalogue.readEntryForIndex(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx).state

        protected val columns: Array<ColumnDef<*>>
            get() = DefaultCatalogue.readEntryForIndex(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx).columns.map {
                DefaultCatalogue.readEntryForColumn(it, this@AbstractIndex.catalogue,  this.context.xodusTx).toColumnDef()
            }.toTypedArray()

        /**
         * Clears the [AbstractTx] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() {
            /* Truncate and replace store.*/
            this@AbstractIndex.catalogue.environment.truncateStore(this@AbstractIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@AbstractIndex.catalogue.environment.openStore(this@AbstractIndex.name.storeName(),this@AbstractIndex.type.storeConfig(), this.context.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@AbstractIndex.name} (${this@AbstractIndex.type}) is missing.")

            /* Update catalogue entry for index. */
            val entry = DefaultCatalogue.readEntryForIndex(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx)
            DefaultCatalogue.writeEntryForIndex(entry.copy(state = IndexState.STALE), this@AbstractIndex.catalogue, this.context.xodusTx)
        }

        /**
         * Checks if this [IndexTx] can process the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = this@AbstractIndex.canProcess(predicate)
    }
}
