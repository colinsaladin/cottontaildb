package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.io.ByteArrayInputStream

/**
 * A [SchemaCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store metadata about [Schema]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class SchemaCatalogueEntry(val name: Name.SchemaName): Comparable<SchemaCatalogueEntry> {

    companion object: ComparableBinding() {

        /** Name of the [SchemaCatalogueEntry] store in this [DefaultCatalogue]. */
        private const val CATALOGUE_SCHEMA_STORE_NAME: String = "ctt_cat_schemas"

        /**
         * Returns the [Store] for [SchemaCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Store {
            return catalogue.environment.openStore(CATALOGUE_SCHEMA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for schema catalogue.")
        }

        /**
         * Initializes the store used to store [SchemaCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()) {
            catalogue.environment.openStore(CATALOGUE_SCHEMA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create schema catalogue store.")
        }

        /**
         * Reads the [SchemaCatalogueEntry] for the given [Name.SchemaName] from the given [DefaultCatalogue].
         *
         * @param name [Name.EntityName] to retrieve the [EntityCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [EntityCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [EntityCatalogueEntry]
         */
        internal fun read(name: Name.SchemaName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): SchemaCatalogueEntry? {
            val rawEntry = store(catalogue, transaction).get(transaction, Name.SchemaName.objectToEntry(name))
            return if (rawEntry != null) {
                SchemaCatalogueEntry.entryToObject(rawEntry) as SchemaCatalogueEntry
            } else {
                null
            }
        }

        /**
         * Reads the [SchemaCatalogueEntry] for the given [Name.SchemaName] from the given [DefaultCatalogue].
         *
         * @param name [Name.EntityName] to retrieve the [EntityCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [EntityCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [EntityCatalogueEntry]
         */
        internal fun exists(name: Name.SchemaName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).get(transaction, Name.SchemaName.objectToEntry(name)) != null

        /**
         * Writes the given [SchemaCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [EntityCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [EntityCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: SchemaCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).put(transaction, Name.SchemaName.objectToEntry(entry.name), SchemaCatalogueEntry.objectToEntry(entry))

        /**
         * Deletes the [SchemaCatalogueEntry] for the given [Name.SchemaName] from the given [DefaultCatalogue].
         *
         * @param name [Name.SchemaName] of the [SchemaCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [SchemaCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.SchemaName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean
            = store(catalogue, transaction).delete(transaction, Name.SchemaName.objectToEntry(name))

        override fun readObject(stream: ByteArrayInputStream) = SchemaCatalogueEntry(Name.SchemaName(StringBinding.BINDING.readObject(stream)))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is SchemaCatalogueEntry) { "$`object` cannot be written as schema entry." }
            StringBinding.BINDING.writeObject(output, `object`.name.components[1])
        }
    }
    override fun compareTo(other: SchemaCatalogueEntry): Int = this.name.toString().compareTo(other.name.toString())
}