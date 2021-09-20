package org.vitrivr.cottontail.database.column

import jetbrains.exodus.bindings.ByteBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.util.ByteIterableUtil
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.catalogue.entries.StatisticsCatalogueEntry
import org.vitrivr.cottontail.database.catalogue.storeName
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.Cursor
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.legacy.v2.column.ColumnV2
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.model.basics.Type
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

    /** The special value that is being interpreted as NULL for this column. */
    private val nullValue = if (columnDef.type == Type.Byte) {
        IntegerBinding.BINDING.objectToEntry(Integer.MIN_VALUE)
    } else {
        ByteBinding.BINDING.objectToEntry(Byte.MIN_VALUE)
    }

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

           this@DefaultColumn.catalogue.environment.statistics
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
         * Returns the number of entries in this [DefaultColumn].
         *
         * @return Number of entries in this [DefaultColumn].
         */
        override fun count(): Long  = this.txLatch.withLock { this.dataStore.count(this.context.xodusTx) }

        /**
         * Gets and returns an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         * @throws DatabaseException If the tuple with the desired ID is invalid.
         */
        override fun get(tupleId: TupleId): T? = this.txLatch.withLock {
            val ret = this.dataStore.get(this.context.xodusTx, tupleId.toKey()) ?: throw java.lang.IllegalArgumentException("Tuple $tupleId does not exist on column ${this@DefaultColumn.name}.")
            if (ByteIterableUtil.compare(this@DefaultColumn.nullValue, ret) == 0) {
                null
            } else {
                this.binding.entryToValue(ret)
            }
        }

        /**
         *
         */
        override fun add(tupleId: TupleId, value: T?): Boolean {
            val rawTuple = tupleId.toKey()
            val valueRaw = value?.let { this.binding.valueToEntry(value) } ?: this@DefaultColumn.nullValue
            if (this.dataStore.add(this.context.xodusTx, rawTuple, valueRaw)) {
                this.statistics.insert(value)
                return true
            }
            return false
        }

        /**
         * Updates the entry with the specified [TupleId] and sets it to the new [Value].
         *
         * @param tupleId The [TupleId] of the entry that should be updated.
         * @param value The new [Value]
         * @return The old [Value]
         */
        override fun update(tupleId: TupleId, value: T?): T? = this.txLatch.withLock {
            /* Read existing value. */
            val rawTuple = tupleId.toKey()
            val valueRaw = value?.let { this.binding.valueToEntry(value) } ?: this@DefaultColumn.nullValue
            val existingRaw = this.dataStore.get(this.context.xodusTx, rawTuple) ?: throw IllegalArgumentException("Cannot update tuple $tupleId because it does not exist.")
            val existing = if (ByteIterableUtil.compare(this@DefaultColumn.nullValue, existingRaw) == 0) {
                null
            } else {
                this.binding.entryToValue(existingRaw)
            }

            /* Perform PUT and update statistics. */
            if (!this.dataStore.put(this.context.xodusTx, rawTuple, valueRaw)) {
                throw DatabaseException.DataCorruptionException("Failed to PUT tuple $tupleId to column ${this@DefaultColumn.name}.")
            }
            this.statistics.update(existing, value)

            /* Return updated value. */
            this.dirty = true
            return existing
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
        override fun cursor(start: TupleId): Cursor<T?> = this.txLatch.withLock {
            object : Cursor<T?> {
                /** Internal [Cursor] used for iteration. */
                private val cursor: jetbrains.exodus.env.Cursor = this@Tx.dataStore.openCursor(this@Tx.context.xodusTx)

                /** The [TupleId] this [Cursor] currently points to. */
                private var tid: TupleId

                /** The [Value] this [Cursor] currently points to. */
                private var value: T? = null

                init {
                    if (start > -1L) {
                        this.cursor.getSearchKeyRange(start.toKey())
                        this.tid = LongBinding.compressedEntryToLong(this.cursor.key)
                        this.value = this@Tx.binding.entryToValue(this.cursor.value)
                    } else {
                        this.tid = -1L
                    }
                }

                override fun moveNext(): Boolean {
                    if (this.cursor.next) {
                        this.tid = LongBinding.compressedEntryToLong(this.cursor.key)
                        this.value = if (ByteIterableUtil.compare(this@DefaultColumn.nullValue, this.cursor.value) == 0) {
                            null
                        } else {
                            this@Tx.binding.entryToValue(this.cursor.value)
                        }
                        return true
                    }
                    return false
                }
                override fun key(): TupleId = this.tid
                override fun value(): T? = this.value
                override fun close() = this.cursor.close()
            }
        }

        /**
         * Called when a transactions commits. Updates [StatisticsCatalogueEntry].
         */
        override fun beforeCommit() {
            /* Update statistics if there have been changes. */
            val entry = StatisticsCatalogueEntry.read(this@DefaultColumn.name, this@DefaultColumn.catalogue, this.context.xodusTx)
                ?: throw DatabaseException.DataCorruptionException("Failed to DELETE value from ${this@DefaultColumn.name}: Reading column statistics failed.")
            StatisticsCatalogueEntry.write(entry.copy(statistics = this.statistics), this@DefaultColumn.catalogue, this.context.xodusTx)
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