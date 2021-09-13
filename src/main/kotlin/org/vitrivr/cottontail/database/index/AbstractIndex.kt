package org.vitrivr.cottontail.database.index

import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.catalogue.entries.ColumnCatalogueEntry
import org.vitrivr.cottontail.database.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.sort.SortOrder
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

    /** A key presentation of this [Name.IndexName]. */
    protected val nameKey = Name.IndexName.Binding.objectToEntry(this.name)

    /** The [ColumnDef] that are covered (i.e. indexed) by this [AbstractIndex]. */
    override val columns: Array<ColumnDef<*>>
        get() {
            val env = this.parent.parent.parent.environment
            return env.computeInTransaction { tx ->
                val indexCatalogue = env.openStore(DefaultCatalogue.CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, it, false)
                    ?: throw DatabaseException.DataCorruptionException("Failed to open index catalogue.")
                val columnCatalogue = env.openStore(DefaultCatalogue.CATALOGUE_COLUMN_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, it, false)
                    ?: throw DatabaseException.DataCorruptionException("Failed to open column catalogue.")
                val entryRaw = indexCatalogue.get(tx, nameKey) ?: throw DatabaseException.DataCorruptionException("Failed to find catalogue entry for index $name.")
                val entry = IndexCatalogueEntry.Binding.entryToObject(entryRaw) as IndexCatalogueEntry
                entry.columns.map {
                    val columnEntryRaw = columnCatalogue.get(tx, Name.ColumnName.Binding.objectToEntry(it))  ?: throw DatabaseException.DataCorruptionException("Failed to find catalogue entry for column $it.")
                    (ColumnCatalogueEntry.Binding.entryToObject(columnEntryRaw) as ColumnCatalogueEntry).toColumnDef()
                }.toTypedArray()
            }
        }

    /** Flag indicating, if this [AbstractIndex] reflects all changes done to the [DefaultEntity]it belongs to. */
    override val state: IndexState
        get()  {
            val env = this.parent.parent.parent.environment
            return env.computeInTransaction {
                val entryRaw = env.openStore(DefaultCatalogue.CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, it, false)?.get(it, nameKey)
                    ?: throw DatabaseException.DataCorruptionException("Failed to find catalogue entry for index $name.")
                val entry = IndexCatalogueEntry.Binding.entryToObject(entryRaw) as IndexCatalogueEntry
                entry.state
            }
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
            get() {
                val env = this@AbstractIndex.parent.parent.parent.environment
                val store = env.openStore(DefaultCatalogue.CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx, false)
                    ?: throw DatabaseException.DataCorruptionException("Failed to open index catalogue.")
                  val entryRaw = store.get(this.context.xodusTx, this@AbstractIndex.nameKey)
                    ?: throw DatabaseException.DataCorruptionException("Failed to find catalogue entry for index $name.")
                val entry = IndexCatalogueEntry.Binding.entryToObject(entryRaw) as IndexCatalogueEntry
                return entry.state
            }

        /**
         * Checks if this [IndexTx] can process the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = this@AbstractIndex.canProcess(predicate)

        /**
         * Releases the [closeLock] in the [AbstractIndex].
         */
        override fun cleanup() {}
    }
}
