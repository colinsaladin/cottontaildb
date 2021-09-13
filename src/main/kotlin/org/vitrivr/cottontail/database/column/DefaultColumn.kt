package org.vitrivr.cottontail.database.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.catalogue.entries.StatisticsCatalogueEntry
import org.vitrivr.cottontail.database.catalogue.storeName
import org.vitrivr.cottontail.legacy.v2.column.ColumnV2
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding

/**
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultColumn<T : Value>(override val columnDef: ColumnDef<T>, override val parent: DefaultEntity) : Column<T> {

    /** The [Name.ColumnName] of this [DefaultColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** Internal representation of the [Name.ColumnName] key. */
    private val nameKey = Name.ColumnName.Binding.objectToEntry(this.name)

    /** Status indicating whether this [DefaultColumn] has been closed. */
    override val closed: Boolean
        get() = this.parent.closed

    /** The [DBOVersion] of this [DefaultColumn]. */
    override val version: DBOVersion
        get() = DBOVersion.V3_0

    /**
     * Creates and returns a new [DefaultColumn.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultColumn.Tx] for.
     * @return New [DefaultColumn.Tx]
     */
    override fun newTx(context: TransactionContext): ColumnTx<T> = Tx(context)

    /**
     * A [Tx] that affects this [ColumnV2].
     */
    inner class Tx constructor(context: TransactionContext) : AbstractTx(context), ColumnTx<T> {

        /** Internal data [Store] reference. */
        private val dataStore: Store = this@DefaultColumn.parent.parent.parent.environment.openStore(
            this@DefaultColumn.name.storeName(),
            StoreConfig.WITHOUT_DUPLICATES,
            this.context.xodusTx,
            false
        ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@DefaultColumn.name} is missing.")

        /** Load internal statistics [Store] reference. */
        private val statisticsStore = this@DefaultColumn.parent.parent.parent.environment.openStore(DefaultCatalogue.CATALOGUE_STATISTICS_STORE_NAME,
            StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING,
            this.context.xodusTx,
            false
        ) ?: throw DatabaseException.DataCorruptionException("Statistics store is missing.")

        /** The internal [XodusBinding] reference used for de-/serialization. */
        private val binding: XodusBinding<T> = this@DefaultColumn.columnDef.type.serializerFactory().xodus(this.columnDef.type.logicalSize)

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: Column<T>
            get() = this@DefaultColumn

        /**
         * Gets and returns [ValueStatistics] for this [ColumnTx]
         *
         * @return [ValueStatistics].
         */
        override fun statistics(): ValueStatistics<T> {
            val entry = StatisticsCatalogueEntry.Binding.entryToObject(this.statisticsStore.get(this.context.xodusTx, this@DefaultColumn.nameKey)
                ?: throw DatabaseException.DataCorruptionException("No statistics entry for column ${this@DefaultColumn.name} is missing.")) as StatisticsCatalogueEntry
            return entry.statistics as ValueStatistics<T>
        }

        /**
         * Gets and returns an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         * @throws DatabaseException If the tuple with the desired ID is invalid.
         */
        override fun get(tupleId: TupleId): T? {
            val ret = this.dataStore.get(this.context.xodusTx, LongBinding.longToCompressedEntry(tupleId)) ?: return null
            return this.binding.entryToValue(ret)
        }

        /**
         * Updates the entry with the specified [TupleId] and sets it to the new [Value].
         *
         * @param tupleId The [TupleId] of the entry that should be updated.
         * @param value The new [Value]
         * @return The old [Value]
         */
        override fun put(tupleId: TupleId, value: T): T? {
            /* Read existing value. */
            val existing = this.dataStore.get(this.context.xodusTx, LongBinding.longToCompressedEntry(tupleId))?.let { this.binding.entryToValue(it) }

            /* Read and update statistics. */
            val entry = StatisticsCatalogueEntry.Binding.entryToObject(
                this.statisticsStore.get(this.context.xodusTx, this@DefaultColumn.nameKey) ?: throw DatabaseException.DataCorruptionException("Statistics entry for column ${this@DefaultColumn.name} is missing.")) as StatisticsCatalogueEntry
            if (existing == null) {
                (entry.statistics as ValueStatistics<T>).insert(value)
            } else {
                (entry.statistics as ValueStatistics<T>).update(existing, value)
            }
            this.statisticsStore.put(this.context.xodusTx, this@DefaultColumn.nameKey, StatisticsCatalogueEntry.Binding.objectToEntry(entry))

            /* Update value. */
            this.dataStore.put(this.context.xodusTx, LongBinding.longToCompressedEntry(tupleId), this.binding.objectToEntry(value))
            return existing
        }

        /**
         * Updates the entry with the specified [TupleId] and sets it to the new [Value] if, and only if, it currently holds the expected [Value].
         *
         * @param tupleId The ID of the record that should be updated
         * @param value The new [Value].
         * @param expected The [Value] expected to be there.
         */
        override fun compareAndPut(tupleId: TupleId, value: T, expected: T?): Boolean {
            val existing = this.dataStore.get(this.context.xodusTx, LongBinding.longToCompressedEntry(tupleId))?.let { this.binding.entryToValue(it) }
            return if (existing == expected) {
                this.put(tupleId, value)
                true
            } else {
                false
            }
        }

        /**
         * Deletes the entry with the specified [TupleId] and sets it to the new value.
         *
         * @param tupleId The ID of the record that should be updated
         * @return The old [Value]
         */
        override fun delete(tupleId: TupleId): T? {
            /* Read existing value. */
            val existing = this.dataStore.get(this.context.xodusTx, LongBinding.longToCompressedEntry(tupleId))?.let { this.binding.entryToValue(it) }

            /* Read and update statistics. */
            val statisticsEntry = StatisticsCatalogueEntry.Binding.entryToObject(this.statisticsStore.get(this.context.xodusTx, this@DefaultColumn.nameKey)
                ?: throw DatabaseException.DataCorruptionException("Statistics entry for column ${this@DefaultColumn.name} is missing.")) as StatisticsCatalogueEntry
            if (existing != null) {
                (statisticsEntry.statistics as ValueStatistics<T>).delete(existing)
                this.statisticsStore.put(this.context.xodusTx, this@DefaultColumn.nameKey, StatisticsCatalogueEntry.Binding.objectToEntry(statisticsEntry))
                this.dataStore.delete(this.context.xodusTx, LongBinding.longToCompressedEntry(tupleId))
            }

            /* Return existing value. */
            return existing
        }

        override fun cleanup() {
            TODO("Not yet implemented")
        }


    }
}