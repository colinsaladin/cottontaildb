package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.*
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.io.ByteArrayInputStream

/**
 * A [ColumnCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store metadata about [Column]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class ColumnCatalogueEntry(val name: Name.ColumnName, val type: Type<*>, val nullable: Boolean, val primary: Boolean): Comparable<ColumnCatalogueEntry> {
    /**
     * Creates a [ColumnCatalogueEntry] from the provided [ColumnDef].
     *
     * @param [ColumnDef] to convert.
     */
    constructor(def: ColumnDef<*>) : this(def.name, def.type, def.primary, def.nullable)

    companion object: ComparableBinding() {

        /** Name of the [ColumnCatalogueEntry] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_COLUMN_STORE_NAME: String = "ctt_cat_columns"

        /**
         * Returns the [Store] for [ColumnCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Store =
            catalogue.environment.openStore(CATALOGUE_COLUMN_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for column catalogue.")

        /**
         * Initializes the store used to store [IndexCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()) {
            catalogue.environment.openStore(CATALOGUE_COLUMN_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create entity catalogue store.")
        }

        /**
         * Reads the [ColumnCatalogueEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param name [Name.ColumnName] to retrieve the [ColumnCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [ColumnCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [ColumnCatalogueEntry]
         */
        internal fun read(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): ColumnCatalogueEntry? {
            val rawName = Name.ColumnName.objectToEntry(name)
            val rawEntry = store(catalogue, transaction).get(transaction, rawName)
            return if (rawEntry != null) {
                ColumnCatalogueEntry.entryToObject(rawEntry) as ColumnCatalogueEntry
            } else {
                null
            }
        }

        /**
         * Writes the given [ColumnCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [ColumnCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [ColumnCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: ColumnCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).put(transaction, Name.ColumnName.objectToEntry(entry.name), ColumnCatalogueEntry.objectToEntry(entry))

        /**
         * Deletes the [ColumnCatalogueEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param name [Name.ColumnName] of the [ColumnCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [ColumnCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).delete(transaction, Name.ColumnName.objectToEntry(name))

        override fun readObject(stream: ByteArrayInputStream): ColumnCatalogueEntry = ColumnCatalogueEntry(
            Name.ColumnName.readObject(stream),
            Type.forOrdinal(IntegerBinding.readCompressed(stream), IntegerBinding.readCompressed(stream)),
            BooleanBinding.BINDING.readObject(stream),
            BooleanBinding.BINDING.readObject(stream),
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is ColumnCatalogueEntry) { "$`object` cannot be written as column entry." }
            Name.ColumnName.Binding.writeObject(output, `object`.name)
            IntegerBinding.writeCompressed(output, `object`.type.ordinal)
            IntegerBinding.writeCompressed(output, `object`.type.logicalSize)
            BooleanBinding.BINDING.writeObject(output, `object`.nullable)
            BooleanBinding.BINDING.writeObject(output, `object`.primary)
        }
    }

    /**
     * Converts this [ColumnCatalogueEntry] to a [ColumnDef].
     *
     * @return [ColumnDef] for this [ColumnCatalogueEntry]
     */
    fun toColumnDef(): ColumnDef<*> = ColumnDef(this.name, this.type, this.nullable, this.primary)
    override fun compareTo(other: ColumnCatalogueEntry): Int = this.name.compareTo(other.name)
}