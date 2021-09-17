package org.vitrivr.cottontail.database.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.log.Log
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
import org.vitrivr.cottontail.model.basics.toKey
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding
import kotlin.concurrent.withLock

/**
 * The default [ColumnDef] implementation based on JetBrains Xodus.
 *
 * @see Column
 * @see ColumnTx
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultColumn<T : Value>(override val columnDef: ColumnDef<T>, override val parent: DefaultEntity) : Column<T> {

    /** The [Name.ColumnName] of this [DefaultColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** Status indicating whether this [DefaultColumn] has been closed. */
    override val closed: Boolean
        get() = this.parent.closed

    /** A [DefaultColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

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
        private var dataStore: Store = this@DefaultColumn.catalogue.environment.openStore(
            this@DefaultColumn.name.storeName(),
            StoreConfig.USE_EXISTING,
            this.context.xodusTx,
            false
        ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@DefaultColumn.name} is missing.")

        /** The internal [XodusBinding] reference used for de-/serialization. */
        private val binding: XodusBinding<T> = this@DefaultColumn.columnDef.type.serializerFactory().xodus(this.columnDef.type.logicalSize)

        /** Internal reference to the [ValueStatistics] for this [DefaultColumn]. */
        private val statistics: ValueStatistics<T> = (StatisticsCatalogueEntry.read(this@DefaultColumn.name, this@DefaultColumn.catalogue, this.context.xodusTx)?.statistics
            ?: throw DatabaseException.DataCorruptionException("Failed to PUT value from ${this@DefaultColumn.name}: Reading column statistics failed.")) as ValueStatistics<T>

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: DefaultColumn<T>
            get() = this@DefaultColumn

        /**
         * Obtains a global (non-exclusive) read-lock on [DefaultCatalogue].
         *
         * Prevents [DefaultCatalogue] from being closed while transaction is ongoing.
         */
        private val closeStamp = this.dbo.catalogue.closeLock.readLock()

        /** Flag indicating that changes have been made through this [DefaultColumn.Tx] */
        @Volatile
        private var dirty: Boolean = false

        init {
            /** Checks if DBO is still open. */
            if (this.dbo.closed) {
                this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this.dbo)
            }
        }

        /**
         * Gets and returns [ValueStatistics] for this [ColumnTx]
         *
         * @return [ValueStatistics].
         */
        override fun statistics(): ValueStatistics<T> = this.txLatch.withLock {
            this.statistics
        }

        /**
         * Gets and returns an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         * @throws DatabaseException If the tuple with the desired ID is invalid.
         */
        override fun get(tupleId: TupleId): T? = this.txLatch.withLock {
            val ret = this.dataStore.get(this.context.xodusTx, tupleId.toKey()) ?: return null
            return this.binding.entryToValue(ret)
        }

        /**
         * Updates the entry with the specified [TupleId] and sets it to the new [Value].
         *
         * @param tupleId The [TupleId] of the entry that should be updated.
         * @param value The new [Value]
         * @return The old [Value]
         */
        override fun put(tupleId: TupleId, value: T): T? = this.txLatch.withLock {
            /* Read existing value. */
            val existing = this.dataStore.get(this.context.xodusTx, tupleId.toKey())?.let { this.binding.entryToValue(it) }

            /* Perform PUT and update statistics. */
            if (existing == null) {
                if (!this.dataStore.add(this.context.xodusTx, tupleId.toKey(), this.binding.objectToEntry(value))) {
                    throw DatabaseException.DataCorruptionException("Failed to ADD tuple $tupleId to column ${this@DefaultColumn.name}.")
                }
                this.dirty = true
                this.statistics.insert(value)
            } else {
                if (!this.dataStore.put(this.context.xodusTx, tupleId.toKey(), this.binding.objectToEntry(value))) {
                    throw DatabaseException.DataCorruptionException("Failed to PUT tuple $tupleId to column ${this@DefaultColumn.name}.")
                }
                this.dirty = true
                this.statistics.update(existing, value)
            }

            /* Update value. */
            return existing
        }

        /**
         * Updates the entry with the specified [TupleId] and sets it to the new [Value] if, and only if, it currently holds the expected [Value].
         *
         * @param tupleId The ID of the record that should be updated
         * @param value The new [Value].
         * @param expected The [Value] expected to be there.
         */
        override fun compareAndPut(tupleId: TupleId, value: T, expected: T?): Boolean = this.txLatch.withLock {
            val existing = this.dataStore.get(this.context.xodusTx, tupleId.toKey())?.let { this.binding.entryToValue(it) }
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
        override fun delete(tupleId: TupleId): T? = this.txLatch.withLock {
            /* Read existing value. */
            val existing = this.dataStore.get(this.context.xodusTx, tupleId.toKey())?.let { this.binding.entryToValue(it) }

            /* Delete entry and update statistics. */
            if (existing != null) {
                if (!this.dataStore.delete(this.context.xodusTx, tupleId.toKey())) {
                    throw DatabaseException.DataCorruptionException("Failed to DELETE tuple $tupleId to column ${this@DefaultColumn.name}.")
                }
                this.dirty = true
                this.statistics.delete(existing)
            }

            /* Return existing value. */
            return existing
        }

        /**
         * Opens a new [Cursor] for this [DefaultColumn.Tx].
         *
         * @param start The [TupleId] to start the [Cursor] at.
         * @return [Cursor]
         */
        override fun cursor(start: TupleId): org.vitrivr.cottontail.database.general.Cursor<T> = this.txLatch.withLock {
                object : org.vitrivr.cottontail.database.general.Cursor<T> {
                /** The maximum [TupleId] to iterate over. */
                private var current: TupleId

                /** Internal [Cursor] used for iteration. */
                private val cursor: Cursor = this@Tx.dataStore.openCursor(this@Tx.context.xodusTx)

                init {
                    if (start > -1L) {
                        this.cursor.getSearchKeyRange(start.toKey())
                        this.current = LongBinding.compressedEntryToLong(this.cursor.key)
                    } else {
                        this.current = start
                    }
                }

                override fun moveNext(): Boolean {
                    if (this.cursor.next) {
                        this.current = LongBinding.compressedEntryToLong(this.cursor.key)
                        return true
                    }
                    return false
                }
                override fun key(): TupleId = this.current
                override fun value(): T = this@Tx.binding.entryToValue(this.cursor.value)
                override fun close() = this.cursor.close()
            }
        }

        /**
         * Called when a transactions commits. Updates [StatisticsCatalogueEntry].
         */
        override fun beforeCommit() {
            /* Update statistics if there have been changes. */
            if (this.dirty) {
                val entry = StatisticsCatalogueEntry.read(this@DefaultColumn.name, this@DefaultColumn.catalogue, this.context.xodusTx)
                    ?: throw DatabaseException.DataCorruptionException("Failed to DELETE value from ${this@DefaultColumn.name}: Reading column statistics failed.")
                if (!StatisticsCatalogueEntry.write(entry.copy(statistics = this.statistics), this@DefaultColumn.catalogue, this.context.xodusTx)) {
                    throw DatabaseException.DataCorruptionException("Failed to PUT value from ${this@DefaultColumn.name}: Update of column statistics failed.")
                }
            }
            super.beforeCommit()
        }

        /**
         * Called when a transaction finalizes. Releases the lock held on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}