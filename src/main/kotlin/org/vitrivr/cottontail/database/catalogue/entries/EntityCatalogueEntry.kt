package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.*
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.io.ByteArrayInputStream

/**
 * A [EntityCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store metadata about [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityCatalogueEntry(val name: Name.EntityName, val created: Long, val columns: List<Name.ColumnName>, val indexes: List<Name.IndexName>): Comparable<EntityCatalogueEntry> {


    companion object: ComparableBinding() {

        /** Name of the [EntityCatalogueEntry] store in this [DefaultCatalogue]. */
        private const val CATALOGUE_ENTITY_STORE_NAME: String = "ctt_cat_entities"

        /**
         * Returns the [Store] for [EntityCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Store {
            return catalogue.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for entity catalogue.")
        }

        /**
         * Initializes the store used to store [EntityCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()) {
            catalogue.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create entity catalogue.")
        }

        /**
         * Reads the [EntityCatalogueEntry] for the given [Name.EntityName] from the given [DefaultCatalogue].
         *
         * @param name [Name.EntityName] to retrieve the [EntityCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [EntityCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [EntityCatalogueEntry]
         */
        internal fun read(name: Name.EntityName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): EntityCatalogueEntry? {
            val rawEntry = store(catalogue, transaction).get(transaction, Name.EntityName.objectToEntry(name))
            return if (rawEntry != null) {
                EntityCatalogueEntry.entryToObject(rawEntry) as EntityCatalogueEntry
            } else {
                null
            }
        }

        /**
         * Checks if the [EntityCatalogueEntry] for the given [Name.EntityName] exists.
         *
         * @param name [Name.EntityName] to check.
         * @param catalogue [DefaultCatalogue] to retrieve [EntityCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return true of false
         */
        internal fun exists(name: Name.EntityName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).get(transaction, Name.EntityName.objectToEntry(name)) != null

        /**
         * Writes the given [EntityCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [EntityCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [EntityCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: EntityCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).put(transaction, Name.EntityName.objectToEntry(entry.name), EntityCatalogueEntry.objectToEntry(entry))

        /**
         * Deletes the [EntityCatalogueEntry] for the given [Name.SchemaName] from the given [DefaultCatalogue].
         *
         * @param name [Name.EntityName] of the [EntityCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [EntityCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.EntityName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean
                = store(catalogue, transaction).delete(transaction, Name.EntityName.objectToEntry(name))

        override fun readObject(stream: ByteArrayInputStream): Comparable<Nothing> {
            val entityName = Name.EntityName.readObject(stream)
            val created = LongBinding.BINDING.readObject(stream)
            val columns = (0 until ShortBinding.BINDING.readObject(stream)).map {
                Name.ColumnName.readObject(stream)
            }
            val indexes = (0 until ShortBinding.BINDING.readObject(stream)).map {
                Name.IndexName.readObject(stream)
            }
            return EntityCatalogueEntry(entityName, created, columns, indexes)
        }

        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is EntityCatalogueEntry) { "$`object` cannot be written as entity entry." }
            Name.EntityName.writeObject(output, `object`.name)
            LongBinding.writeCompressed(output, `object`.created)

            /* Write all columns. */
            ShortBinding.BINDING.writeObject(output,`object`.columns.size)
            for (columnName in `object`.columns) {
                Name.ColumnName.writeObject(output, columnName)
            }

            /* Write all indexes. */
            ShortBinding.BINDING.writeObject(output,`object`.indexes.size)
            for (indexName in `object`.indexes) {
                Name.IndexName.writeObject(output, indexName)
            }
        }
    }

    override fun compareTo(other: EntityCatalogueEntry): Int = this.name.toString().compareTo(other.name.toString())
}