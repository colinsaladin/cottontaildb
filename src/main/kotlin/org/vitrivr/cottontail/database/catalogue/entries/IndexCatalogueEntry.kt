package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.basics.IndexState
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.io.ByteArrayInputStream

/**
 * A [IndexCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store metadata about [Index]es.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class IndexCatalogueEntry(val name: Name.IndexName, val type: IndexType, val state: IndexState, val columns: Array<Name.ColumnName>, val config: Map<String,String>): Comparable<IndexCatalogueEntry> {
    companion object: ComparableBinding() {
        /** Name of the [IndexCatalogueEntry] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_INDEX_STORE_NAME: String = "ctt_cat_indexes"

        /**
         * Initializes the store used to store [IndexCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()) {
            catalogue.environment.openStore(CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create store for index catalogue.")
        }

        /**
         * Returns the [Store] for [IndexCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to retrieve [IndexCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Store =
            catalogue.environment.openStore(CATALOGUE_INDEX_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for index catalogue.")

        /**
         * Reads the [IndexCatalogueEntry] for the given [Name.IndexName] from the given [DefaultCatalogue].
         *
         * @param name [Name.IndexName] to retrieve the [IndexCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [IndexCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         */
        internal fun read(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): IndexCatalogueEntry? {
            val rawEntry = store(catalogue, transaction).get(transaction, Name.IndexName.objectToEntry(name))
            return if (rawEntry != null) {
                entryToObject(rawEntry) as IndexCatalogueEntry
            } else {
                null
            }
        }

        /**
         * Checks if the [IndexCatalogueEntry] for the given [Name.IndexName] exists.
         *
         * @param name [Name.IndexName] to check.
         * @param catalogue [DefaultCatalogue] to retrieve [IndexCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         */
        internal fun exists(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).get(transaction, Name.IndexName.objectToEntry(name)) != null

        /**
         * Writes the given [IndexCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [IndexCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [IndexCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: IndexCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).put(transaction, Name.IndexName.objectToEntry(entry.name), objectToEntry(entry))

        /**
         * Deletes the [IndexCatalogueEntry] for the given [Name.IndexName] from the given [DefaultCatalogue].
         *
         * @param name [Name.IndexName] of the [IndexCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [IndexCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).delete(transaction, Name.IndexName.objectToEntry(name))

        override fun readObject(stream: ByteArrayInputStream): IndexCatalogueEntry {
            val entityName = Name.IndexName.readObject(stream)
            val type = IndexType.values()[IntegerBinding.readCompressed(stream)]
            val state = IndexState.values()[IntegerBinding.readCompressed(stream)]
            val columns = (0 until ShortBinding.BINDING.readObject(stream)).map {
                Name.ColumnName.readObject(stream)
            }.toTypedArray()
            val entries = (0 until ShortBinding.BINDING.readObject(stream)).associate {
                StringBinding.BINDING.readObject(stream) to StringBinding.BINDING.readObject(stream)
            }
            return IndexCatalogueEntry(entityName, type, state, columns, entries)
        }

        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is IndexCatalogueEntry) { "$`object` cannot be written as index entry." }
            Name.EntityName.writeObject(output, `object`.name)
            IntegerBinding.writeCompressed(output, `object`.type.ordinal)
            IntegerBinding.writeCompressed(output, `object`.state.ordinal)

            /* Write all columns. */
            ShortBinding.BINDING.writeObject(output,`object`.columns.size)
            for (columnName in `object`.columns) {
                Name.ColumnName.writeObject(output, columnName)
            }

            /* Write all indexes. */
            ShortBinding.BINDING.writeObject(output,`object`.config.size)
            for (entry in `object`.config) {
                StringBinding.BINDING.writeObject(output, entry.key)
                StringBinding.BINDING.writeObject(output, entry.value)
            }
        }
    }

    override fun compareTo(other: IndexCatalogueEntry): Int = this.name.toString().compareTo(other.name.toString())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as IndexCatalogueEntry

        if (name != other.name) return false
        if (type != other.type) return false
        if (state != other.state) return false
        if (!columns.contentEquals(other.columns)) return false
        if (config != other.config) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + state.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + config.hashCode()
        return result
    }
}