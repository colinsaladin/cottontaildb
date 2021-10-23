package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.statistics.columns.*
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.io.ByteArrayInputStream

/**
 * A [StatisticsCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store statics about [Column]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class StatisticsCatalogueEntry(val name: Name.ColumnName, val type: Type<*>, val statistics: ValueStatistics<*>)  {
    /**
     * Creates a [StatisticsCatalogueEntry] from the provided [ColumnDef].
     *
     * @param def The [ColumnDef] to convert.
     */
    constructor(def: ColumnDef<*>) : this(def.name, def.type, when(def.type){
        Type.Boolean -> BooleanValueStatistics()
        Type.Byte -> ByteValueStatistics()
        Type.Short -> ShortValueStatistics()
        Type.Date -> DateValueStatistics()
        Type.Double -> DoubleValueStatistics()
        Type.Float -> FloatValueStatistics()
        Type.Int -> IntValueStatistics()
        Type.Long -> LongValueStatistics()
        Type.String -> StringValueStatistics()
        is Type.BooleanVector -> BooleanVectorValueStatistics(def.type)
        is Type.DoubleVector -> DoubleVectorValueStatistics(def.type)
        is Type.FloatVector -> FloatVectorValueStatistics(def.type)
        is Type.IntVector -> IntVectorValueStatistics(def.type)
        is Type.LongVector -> LongVectorValueStatistics(def.type)
        else -> ValueStatistics(def.type)
    })

    /**
     * Creates a [Serialized] version of this [StatisticsCatalogueEntry].
     *
     * @return [Serialized]
     */
    private fun toSerialized() = Serialized(this.type, this.statistics)

    /**
     * The [Serialized] version of the [StatisticsCatalogueEntry]. That entry does not include the [Name.ColumnName].
     */
    private data class Serialized(val type: Type<*>, val statistics: ValueStatistics<*>): Comparable<Serialized> {

        /**
         * Converts this [Serialized] to an actual [StatisticsCatalogueEntry].
         *
         * @param name The [Name.ColumnName] this entry belongs to.
         * @return [StatisticsCatalogueEntry]
         */
        fun toActual(name: Name.ColumnName) = StatisticsCatalogueEntry(name, this.type, this.statistics)

        companion object: ComparableBinding() {
            /**
             * De-serializes a [Serialized] from the given [ByteArrayInputStream].
             */
            override fun readObject(stream: ByteArrayInputStream): Serialized {
                val type = Type.forOrdinal(IntegerBinding.readCompressed(stream), IntegerBinding.readCompressed(stream))
                val statistics = ValueStatistics.read(stream, type)
                return Serialized(type, statistics)
            }

            /**
             * Serializes a [Serialized] to the given [LightOutputStream].
             */
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                require(`object` is Serialized) { "$`object` cannot be written as statistics entry." }
                IntegerBinding.writeCompressed(output, `object`.type.ordinal)
                IntegerBinding.writeCompressed(output, `object`.type.logicalSize)
                ValueStatistics.write(output, `object`.statistics)
            }
        }
        override fun compareTo(other: Serialized): Int = this.type.ordinal.compareTo(other.type.ordinal)
    }


    companion object {

        /** Name of the [StatisticsCatalogueEntry] store in this [DefaultCatalogue]. */
        private const val CATALOGUE_STATISTICS_STORE_NAME: String = "ctt_cat_statistics"

        /**
         * Returns the [Store] for [ColumnCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Store =
            catalogue.environment.openStore(CATALOGUE_STATISTICS_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for column statistics catalogue.")

        /**
         * Initializes the store used to store [SchemaCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()) {
            catalogue.environment.openStore(CATALOGUE_STATISTICS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create column statistics catalogue store.")
        }

        /**
         * Reads the [StatisticsCatalogueEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param name [Name.ColumnName] to retrieve the [StatisticsCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [StatisticsCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [StatisticsCatalogueEntry]
         */
        internal fun read(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): StatisticsCatalogueEntry? {
            val rawEntry = store(catalogue, transaction).get(transaction, Name.ColumnName.objectToEntry(name))
            return if (rawEntry != null) {
                (Serialized.entryToObject(rawEntry) as Serialized).toActual(name)
            } else {
                null
            }
        }

        /**
         * Writes the given [StatisticsCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [StatisticsCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [StatisticsCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: StatisticsCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
             store(catalogue, transaction).put(transaction, Name.ColumnName.objectToEntry(entry.name), Serialized.objectToEntry(entry.toSerialized()))

        /**
         * Deletes the [StatisticsCatalogueEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param name [Name.ColumnName] of the [StatisticsCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [StatisticsCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean =
            store(catalogue, transaction).delete(transaction, Name.ColumnName.objectToEntry(name))
    }
}