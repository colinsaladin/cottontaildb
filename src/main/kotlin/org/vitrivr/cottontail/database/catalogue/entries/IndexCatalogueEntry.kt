package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.*
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
data class IndexCatalogueEntry(val name: Name.IndexName, val type: IndexType, val state: IndexState, val columns: List<Name.ColumnName>, val config: Map<String,String>) {

    /**
     * Creates a [Serialized] version of this [IndexCatalogueEntry].
     *
     * @return [Serialized]
     */
    private fun toSerialized() = Serialized(this.type, this.state, this.columns.map { it.simple }, this.config)

    /**
     * The [Serialized] version of the [IndexCatalogueEntry]. That entry does not include the [Name] objects.
     */
    private data class Serialized(val type: IndexType, val state: IndexState, val columns: List<String>, val config: Map<String, String>): Comparable<Serialized> {

        /**
         * Converts this [Serialized] to an actual [IndexCatalogueEntry].
         *
         * @param name The [Name.IndexName] this entry belongs to.
         * @return [IndexCatalogueEntry]
         */
        fun toActual(name: Name.IndexName) = IndexCatalogueEntry(name, this.type, this.state, this.columns.map { name.entity().column(it) }, this.config)

        companion object: ComparableBinding() {

            /**
             * De-serializes a [Serialized] from the given [ByteArrayInputStream].
             */
            override fun readObject(stream: ByteArrayInputStream): Serialized {
                val type = IndexType.values()[IntegerBinding.readCompressed(stream)]
                val state = IndexState.values()[IntegerBinding.readCompressed(stream)]
                val columns = (0 until ShortBinding.BINDING.readObject(stream)).map {
                    StringBinding.BINDING.readObject(stream)
                }
                val entries = (0 until ShortBinding.BINDING.readObject(stream)).associate {
                    StringBinding.BINDING.readObject(stream) to StringBinding.BINDING.readObject(stream)
                }
                return Serialized(type, state, columns, entries)
            }

            /**
             * Serializes a [Serialized] to the given [LightOutputStream].
             */
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                require(`object` is Serialized) { "$`object` cannot be written as index entry." }
                IntegerBinding.writeCompressed(output, `object`.type.ordinal)
                IntegerBinding.writeCompressed(output, `object`.state.ordinal)

                /* Write all columns. */
                ShortBinding.BINDING.writeObject(output,`object`.columns.size)
                for (columnName in `object`.columns) {
                    StringBinding.BINDING.writeObject(output, columnName)
                }

                /* Write all indexes. */
                ShortBinding.BINDING.writeObject(output,`object`.config.size)
                for (entry in `object`.config) {
                    StringBinding.BINDING.writeObject(output, entry.key)
                    StringBinding.BINDING.writeObject(output, entry.value)
                }
            }
        }
        override fun compareTo(other: Serialized): Int = this.type.ordinal.compareTo(other.type.ordinal)
    }

    companion object {
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
                (Serialized.entryToObject(rawEntry) as Serialized).toActual(name)
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
            store(catalogue, transaction).put(transaction, Name.IndexName.objectToEntry(entry.name), Serialized.objectToEntry(entry.toSerialized()))

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
    }
}