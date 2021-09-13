package org.vitrivr.cottontail.database.catalogue

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.PersistentEntityStores
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.env.forEach
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.entries.*
import org.vitrivr.cottontail.database.general.*
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.functions.FunctionRegistry
import org.vitrivr.cottontail.functions.initialize
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.concurrent.locks.StampedLock

/**
 * The default [Catalogue] implementation based on Map DB.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultCatalogue(override val config: Config) : Catalogue {
    /**
     * Companion object to [DefaultCatalogue]
     */
    companion object {
        /** Name of the Cottontail DB catalogue store. */
        internal const val CATALOGUE_STORE_NAME: String = "cdb_catalogue"

        /** Name of the sequences entry this [DefaultCatalogue]. */
        internal const val CATALOGUE_SEQUENCE_STORE_NAME: String = "ctt_cat_sequences"

        /** Name of the [SchemaCatalogueEntry] store in this [DefaultCatalogue]. */
        internal const val CATALOGUE_SCHEMA_STORE_NAME: String = "ctt_cat_schemas"

        /** Name of the [EntityCatalogueEntry] store in this [DefaultCatalogue]. */
        internal const val CATALOGUE_ENTITY_STORE_NAME: String = "ctt_cat_entities"

        /** Name of the [ColumnCatalogueEntry] store in the Cottontail DB catalogue. */
        internal const val CATALOGUE_COLUMN_STORE_NAME: String = "ctt_cat_columns"

        /** Name of the [IndexCatalogueEntry] store in the Cottontail DB catalogue. */
        internal const val CATALOGUE_INDEX_STORE_NAME: String = "ctt_cat_indexes"

        /** Name of the [StatisticsCatalogueEntry] store in this [DefaultCatalogue]. */
        internal const val CATALOGUE_STATISTICS_STORE_NAME: String = "ctt_cat_statistics"

        /** Prefix used for actual column stores. */
        internal const val ENTITY_STORE_PREFIX: String = "ctt_ent_"

        /** Prefix used for actual column stores. */
        internal const val COLUMN_STORE_PREFIX: String = "ctt_col_"

        /** Prefix used for actual index stores. */
        internal const val INDEX_STORE_PREFIX: String = "ctt_idx_"

        /**
         * Reads and returns an [EntityCatalogueEntry] for the given [Name.ColumnName].
         *
         * @param name [Name.EntityName] to retrieve the [EntityCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [EntityCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [EntityCatalogueEntry]
         */
        internal fun readEntryForEntity(name: Name.EntityName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): EntityCatalogueEntry {
            val rawName = Name.EntityName.objectToEntry(name)
            val store = catalogue.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open entity catalogue.")
            val rawEntry = store.get(transaction, rawName)
            if (rawEntry != null) {
                return EntityCatalogueEntry.entryToObject(rawEntry) as EntityCatalogueEntry
            } else {
                throw DatabaseException.EntityDoesNotExistException(name)
            }
        }

        /**
         * Reads and returns an [EntityCatalogueEntry] for the given [Name.EntityName].
         *
         * @param entry [EntityCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [EntityCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun writeEntryForEntity(entry: EntityCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean {
            val rawName = Name.EntityName.objectToEntry(entry.name)
            val store = catalogue.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open entity catalogue.")
            return store.put(transaction, rawName, EntityCatalogueEntry.objectToEntry(entry))
        }

        /**
         * Reads and returns the entry for the given name without changing it.
         *
         * @param nameKey [ArrayByteIterable] that identifies the sequence entry.
         * @param catalogue [DefaultCatalogue] to retrieve the sequence entry from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [EntityCatalogueEntry]
         */
        internal fun readSequence(nameKey: ArrayByteIterable, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Long? {
            val store = catalogue.environment.openStore(CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open sequence catalogue.")
            val rawEntry = store.get(transaction, nameKey)
            return if (rawEntry != null) {
                LongBinding.entryToLong(rawEntry)
            } else {
                null
            }
        }

        /** Reads, increments and returns the entry for the given name without changing it.
         *
         * @param nameKey [ArrayByteIterable] that identifies the sequence entry.
         * @param catalogue [DefaultCatalogue] to retrieve the sequence entry from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [EntityCatalogueEntry]
         */
        internal fun nextSequence(nameKey: ArrayByteIterable, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Long? {
            val store = catalogue.environment.openStore(CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open sequence catalogue.")
            val rawEntry = store.get(transaction, nameKey)
            return if (rawEntry != null) {
                val next = LongBinding.entryToLong(rawEntry) + 1
                store.put(transaction, nameKey, LongBinding.longToCompressedEntry(next))
                return next
            } else {
                null
            }
        }

        /**
         * Resets the sequence with the given name.
         *
         * @param nameKey [ArrayByteIterable] identifying the sequence to reset.
         * @param catalogue [DefaultCatalogue] to reset the sequence.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success.
         */
        internal fun resetSequence(nameKey: ArrayByteIterable, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean {
            val store = catalogue.environment.openStore(CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open sequence catalogue.")
            return store.put(transaction, nameKey, LongBinding.longToCompressedEntry(0L))
        }

        /**
         * Reads and returns an [ColumnCatalogueEntry] for the given [Name.ColumnName].
         *
         * @param name [Name.ColumnName] to retrieve the [ColumnCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [ColumnCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [ColumnCatalogueEntry]
         */
        internal fun readEntryForColumn(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): ColumnCatalogueEntry {
            val rawName = Name.ColumnName.objectToEntry(name)
            val store = catalogue.environment.openStore(CATALOGUE_COLUMN_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open column catalogue.")
            val rawEntry = store.get(transaction, rawName)
            if (rawEntry != null) {
                return ColumnCatalogueEntry.entryToObject(rawEntry) as ColumnCatalogueEntry
            } else {
                throw DatabaseException.ColumnDoesNotExistException(name)
            }
        }

        /**
         * Reads and returns an [ColumnCatalogueEntry] for the given [Name.IndexName].
         *
         * @param entry [ColumnCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [ColumnCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun writeEntryForColumn(entry: ColumnCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean {
            val rawName = Name.ColumnName.objectToEntry(entry.name)
            val store = catalogue.environment.openStore(CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open column catalogue.")
            return store.put(transaction, rawName, ColumnCatalogueEntry.objectToEntry(entry))
        }

        /**
         * Reads and returns an [StatisticsCatalogueEntry] for the given [Name.ColumnName].
         *
         * @param name [Name.ColumnName] to retrieve the [StatisticsCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [StatisticsCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [StatisticsCatalogueEntry]
         */
        internal fun readEntryForStatistics(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): StatisticsCatalogueEntry {
            val rawName = Name.ColumnName.objectToEntry(name)
            val store = catalogue.environment.openStore(CATALOGUE_STATISTICS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open statistics catalogue.")
            val rawEntry = store.get(transaction, rawName)
            if (rawEntry != null) {
                return StatisticsCatalogueEntry.entryToObject(rawEntry) as StatisticsCatalogueEntry
            } else {
                throw DatabaseException.IndexDoesNotExistException(name)
            }
        }

        /**
         * Reads and returns an [StatisticsCatalogueEntry] for the given [Name.IndexName].
         *
         * @param entry [StatisticsCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [StatisticsCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun writeEntryForStatistics(entry: StatisticsCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean {
            val rawName = Name.ColumnName.objectToEntry(entry.name)
            val store = catalogue.environment.openStore(CATALOGUE_STATISTICS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open statistics catalogue.")
            return store.put(transaction, rawName, StatisticsCatalogueEntry.objectToEntry(entry))
        }

        /**
         * Reads and returns an [IndexCatalogueEntry] for the given [Name.IndexName].
         *
         * @param name [Name.IndexName] to retrieve the [IndexCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [IndexCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         */
        internal fun readEntryForIndex(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): IndexCatalogueEntry {
            val rawName = Name.IndexName.objectToEntry(name)
            val store = catalogue.environment.openStore(CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open index catalogue.")
            val rawEntry = store.get(transaction, rawName)
            if (rawEntry != null) {
                return IndexCatalogueEntry.entryToObject(rawEntry) as IndexCatalogueEntry
            } else {
                throw DatabaseException.IndexDoesNotExistException(name)
            }
        }

        /**
         * Reads and returns an [IndexCatalogueEntry] for the given [Name.IndexName].
         *
         * @param entry [IndexCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [IndexCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun writeEntryForIndex(entry: IndexCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()): Boolean {
            val rawName = Name.IndexName.objectToEntry(entry.name)
            val store = catalogue.environment.openStore(CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open index catalogue.")
            return store.put(transaction, rawName, IndexCatalogueEntry.objectToEntry(entry))
        }
    }

    /** Root to Cottontail DB root folder. */
    override val path: Path = this.config.root

    /** Constant name of the [DefaultCatalogue] object. */
    override val name: Name.RootName
        get() = Name.RootName

    /** The [DBOVersion] of this [DefaultCatalogue]. */
    override val version: DBOVersion
        get() = DBOVersion.V3_0

    /** Constant parent [DBO], which is null in case of the [DefaultCatalogue]. */
    override val parent: DBO? = null

    /** Any [DefaultCatalogue] belongs to itself. */
    override val catalogue: DefaultCatalogue = this

    /** A lock used to mediate access to this [DefaultCatalogue]. */
    private val closeLock = StampedLock()

    /** The Xodus [PersistentEntityStore] used for Cottontail DB. */
    private val entityStore: PersistentEntityStore = PersistentEntityStores.newInstance(this.config.root.toFile())

    /** The Xodus environment used for Cottontail DB. */
    val environment: Environment = this.entityStore.environment

    /** The [FunctionRegistry] exposed by this [Catalogue]. */
    override val functions: FunctionRegistry = FunctionRegistry(this.config)

    /** Status indicating whether this [DefaultCatalogue] is open or closed. */
    override val closed: Boolean
        get() = !this.environment.isOpen

    init {
        /* Initialize function registry. */
        this.functions.initialize()
    }

    /**
     * Creates and returns a new [DefaultCatalogue.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultCatalogue.Tx] for.
     * @return New [DefaultCatalogue.Tx]
     */
    override fun newTx(context: TransactionContext): Tx = Tx(context)

    /**
     * Closes the [DefaultCatalogue] and all objects contained within.
     */
    override fun close() = this.closeLock.write {
        this.environment.close()
    }

    /**
     * A [Tx] that affects this [DefaultCatalogue].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), CatalogueTx {

        /** Reference to the [DefaultCatalogue] this [CatalogueTx] belongs to. */
        override val dbo: DefaultCatalogue
            get() = this@DefaultCatalogue

        /** Obtains a global (non-exclusive) read-lock on [DefaultCatalogue]. Prevents enclosing [Schema] from being closed. */
        private val closeStamp = this@DefaultCatalogue.closeLock.readLock()

        /** Checks if DBO is still open. */
        init {
            if (this@DefaultCatalogue.closed) {
                this@DefaultCatalogue.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this@DefaultCatalogue)
            }
        }

        /**
         * Returns a list of [Name.SchemaName] held by this [DefaultCatalogue].
         *
         * @return [List] of all [Name.SchemaName].
         */
        override fun listSchemas(): List<Name.SchemaName> {
            val store = this@DefaultCatalogue.environment.openStore(CATALOGUE_SCHEMA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx)
            val list = mutableListOf<Name.SchemaName>()
            store.openCursor(this.context.xodusTx).forEach {
                val entry = SchemaCatalogueEntry.Binding.entryToObject(this.value) as SchemaCatalogueEntry
                list.add(entry.name)
            }
            return list
        }

        /**
         * Returns the [Schema] for the given [Name.SchemaName].
         *
         * @param name [Name.SchemaName] to obtain the [Schema] for.
         */
        override fun schemaForName(name: Name.SchemaName): Schema {
            val key = StringBinding.stringToEntry(name.toString())
            val store = this@DefaultCatalogue.environment.openStore(CATALOGUE_SCHEMA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx)
            val entry = SchemaCatalogueEntry.Binding.entryToObject(store.get(this.context.xodusTx, key) ?: throw DatabaseException.SchemaDoesNotExistException(name)) as SchemaCatalogueEntry
            return DefaultSchema(entry.name, this@DefaultCatalogue)
        }

        /**
         * Creates a new, empty [Schema] with the given [Name.SchemaName] and [Path]
         *
         * @param name The [Name.SchemaName] of the new [Schema].
         */
        override fun createSchema(name: Name.SchemaName): Schema {
            val entry = SchemaCatalogueEntry(name)
            val key = StringBinding.stringToEntry(name.toString())
            val value = SchemaCatalogueEntry.Binding.objectToEntry(entry)
            val store = this@DefaultCatalogue.environment.openStore(CATALOGUE_SCHEMA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx)
            if (store.exists(this.context.xodusTx, key, value)) throw DatabaseException.SchemaAlreadyExistsException(name)

            /* Initialize schema on disk */
            store.put(this.context.xodusTx, key, value)
            return DefaultSchema(entry.name, this@DefaultCatalogue)
        }

        /**
         * Drops an existing [Schema] with the given [Name.SchemaName].
         *
         * @param name The [Name.SchemaName] of the [Schema] to be dropped.
         */
        override fun dropSchema(name: Name.SchemaName) {
            /* Obtain schema and check. */
            val key = StringBinding.stringToEntry(name.toString())
            val store = this@DefaultCatalogue.environment.openStore(CATALOGUE_SCHEMA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx)
            val entry = SchemaCatalogueEntry.Binding.entryToObject(store.get(this.context.xodusTx, key) ?: throw DatabaseException.SchemaDoesNotExistException(name)) as SchemaCatalogueEntry

            /* Drop all entities. */
            val schema = DefaultSchema(entry.name, this@DefaultCatalogue)
            val schemaTx = schema.newTx(this.context)
            schemaTx.listEntities().forEach { schemaTx.dropEntity(it) }

            /* Remove entity from list. */
            store.delete(this.context.xodusTx, key)
        }
    }
}