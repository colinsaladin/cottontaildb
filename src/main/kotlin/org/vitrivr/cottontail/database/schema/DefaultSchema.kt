package org.vitrivr.cottontail.database.schema

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.forEach
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue.Companion.CATALOGUE_ENTITY_STORE_NAME
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.catalogue.entries.ColumnCatalogueEntry
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.catalogue.entries.EntityCatalogueEntry
import org.vitrivr.cottontail.database.catalogue.entries.StatisticsCatalogueEntry
import org.vitrivr.cottontail.database.catalogue.storeName
import org.vitrivr.cottontail.database.general.*
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import java.util.*
import javax.xml.crypto.Data

/**
 * Default [Schema] implementation in Cottontail DB based on Map DB.
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultSchema(override val name: Name.SchemaName, override val parent: DefaultCatalogue) : Schema {

    /** The [DBOVersion] of this [DefaultSchema]. */
    override val version: DBOVersion
        get() = DBOVersion.V3_0

    /** Flag indicating whether this [DefaultSchema] has been closed. Depends solely on the parent [Catalogue]. */
    override val closed: Boolean
        get() = this.parent.closed

    /**
     * Creates and returns a new [DefaultSchema.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultSchema.Tx] for.
     * @return New [DefaultSchema.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * A [Tx] that affects this [DefaultSchema].
     *
     * @author Ralph Gasser
     * @version 3.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), SchemaTx {

        /** Reference to the surrounding [DefaultSchema]. */
        override val dbo: DBO
            get() = this@DefaultSchema

        /** Checks if DBO is still open. */
        init {
            if (this@DefaultSchema.closed)
                throw TxException.TxDBOClosedException(this.context.txId, this@DefaultSchema)
        }

        /**
         * Returns a list of all [Name.EntityName]s held by this [DefaultSchema].
         *
         * @return [List] of all [Name.EntityName].
         */
        override fun listEntities(): List<Name.EntityName> {
            val store = this@DefaultSchema.parent.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)
            val list = mutableListOf<Name.EntityName>()
            store.openCursor(this.context.xodusTx).forEach {
                val entry = EntityCatalogueEntry.Binding.entryToObject(this.value) as EntityCatalogueEntry
                list.add(entry.name)
            }
            return list
        }

        /**
         * Returns an [Entity] if such an instance exists.
         *
         * @param name Name of the [Entity] to access.
         * @return [Entity] or null.
         */
        override fun entityForName(name: Name.EntityName): Entity {
            val store = this@DefaultSchema.parent.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx)
            val key = Name.EntityName.Binding.objectToEntry(name)
            val entry = EntityCatalogueEntry.Binding.entryToObject((store.get(this.context.xodusTx, key) ?: throw DatabaseException.EntityDoesNotExistException(name))) as EntityCatalogueEntry
            return DefaultEntity(entry.name, this@DefaultSchema)
        }

        /**
         * Creates a new [DefaultEntity] in this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be created.
         * @param columns The [ColumnDef] of the columns the new [DefaultEntity] should have
         */
        override fun createEntity(name: Name.EntityName, vararg columns: ColumnDef<*>): Entity {
            /* Prepare catalogue entries. */
            val entityKey = Name.EntityName.Binding.objectToEntry(name)
            val entity = EntityCatalogueEntry(name, System.currentTimeMillis(), columns.map { it.name }, emptyList())

            /* Open necessary stores. */
            val entityCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)
            val sequenceCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)
            val statisticsCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_STATISTICS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)
            val columnCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_COLUMN_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)
            val indexCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)

            /* Sanity check: uniqueness of entity and columns. */
            if (entityCatalogue.get(this.context.xodusTx, entityKey) != null) throw DatabaseException.EntityAlreadyExistsException(name)
            val distinctSize = columns.map { it.name }.distinct().size
            if (distinctSize != columns.size) {
                val cols = columns.map { it.name }
                throw DatabaseException.DuplicateColumnException(name, cols)
            }

            /* Add catalogue entries and stores at entity level. */
            if (!entityCatalogue.put(this.context.xodusTx, entityKey, EntityCatalogueEntry.Binding.objectToEntry(entity)))
                throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create catalogue entry.")
            if (!sequenceCatalogue.put(this.context.xodusTx, entityKey, LongBinding.longToEntry(0)))
                throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create tuple ID sequence entry.")
            if (this@DefaultSchema.parent.environment.openStore(name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx, true) == null)
                throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create store for entity.")

            /* Add catalogue entries and stores at column level. */
            columns.forEach {
                val key = Name.ColumnName.Binding.objectToEntry(it.name)

                if (!columnCatalogue.put(this.context.xodusTx, key, ColumnCatalogueEntry.Binding.objectToEntry(ColumnCatalogueEntry(it))))
                    throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create column entry for column $it.")

                if (!statisticsCatalogue.put(this.context.xodusTx, key, StatisticsCatalogueEntry.Binding.objectToEntry(StatisticsCatalogueEntry(it))))
                    throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create statistics entry for column $it.")

                if (this@DefaultSchema.parent.environment.openStore(it.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx, true) == null)
                    throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create store for column $it.")
            }

            /* Return a DefaultEntity instance. */
            return DefaultEntity(name, this@DefaultSchema)
        }

        /**
         * Drops an [DefaultEntity] from this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be dropped.
         */
        override fun dropEntity(name: Name.EntityName) {
            val entityKey = Name.EntityName.Binding.objectToEntry(name)

            /* Open necessary stores. */
            val entityCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)
            val sequenceCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)
            val statisticsCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_STATISTICS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)
            val columnCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_COLUMN_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)
            val indexCatalogue = this@DefaultSchema.parent.environment.openStore(DefaultCatalogue.CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, this.context.xodusTx)

            /* Read entity catalogue entry (does it exist?) */
            val entityEntry = EntityCatalogueEntry.Binding.entryToObject(entityCatalogue.get(this.context.xodusTx, entityKey) ?: throw DatabaseException.EntityDoesNotExistException(name)) as EntityCatalogueEntry

            /* Remove catalogue entries and drop stores.  */
            if (!entityCatalogue.delete(this.context.xodusTx, entityKey))
                throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete catalogue entry.")

            if (!sequenceCatalogue.delete(this.context.xodusTx, entityKey))
                throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete tuple ID sequence entry.")

            this@DefaultSchema.parent.environment.removeStore(name.storeName(), this.context.xodusTx)

            /* Update catalogue entries for related columns. */
            entityEntry.columns.forEach {
                val columnKey = Name.ColumnName.Binding.objectToEntry(it)
                if (!columnCatalogue.delete(this.context.xodusTx, columnKey))
                    throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete column entry for column $it.")

                if (!statisticsCatalogue.delete(this.context.xodusTx, columnKey))
                    throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete statistics entry for column $it.")

                this@DefaultSchema.parent.environment.removeStore(it.storeName(), this.context.xodusTx)
            }

            /* Update catalogue entries for related indexes. */
            entityEntry.indexes.forEach {
                if (!indexCatalogue.delete(this.context.xodusTx, Name.IndexName.Binding.objectToEntry(it)))
                    throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete index entry for column $it.")

                this@DefaultSchema.parent.environment.removeStore(it.storeName(), this.context.xodusTx)
            }
        }

        /**
         * Releases the [closeLock] on the [DefaultSchema].
         */
        override fun cleanup() {}
    }
}



