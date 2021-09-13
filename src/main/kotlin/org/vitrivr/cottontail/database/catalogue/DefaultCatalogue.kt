package org.vitrivr.cottontail.database.catalogue

import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.PersistentEntityStores
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.forEach
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.entries.ColumnCatalogueEntry
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.catalogue.entries.EntityCatalogueEntry
import org.vitrivr.cottontail.database.general.*
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.catalogue.entries.SchemaCatalogueEntry
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

        /** Name of the [IndexEntry] store in the Cottontail DB catalogue. */
        internal const val CATALOGUE_INDEX_STORE_NAME: String = "ctt_cat_indexes"

        /** Name of the [StatisticsEntry] store in this [DefaultCatalogue]. */
        internal const val CATALOGUE_STATISTICS_STORE_NAME: String = "ctt_cat_statistics"

        /** Prefix used for actual column stores. */
        internal const val ENTITY_STORE_PREFIX: String = "ctt_ent_"

        /** Prefix used for actual column stores. */
        internal const val COLUMN_STORE_PREFIX: String = "ctt_col_"

        /** Prefix used for actual index stores. */
        internal const val INDEX_STORE_PREFIX: String = "ctt_idx_"

        /** Filename for the [DefaultEntity] catalogue.  */
        internal const val FILE_CATALOGUE = "catalogue.db"
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

        /**
         * Releases the [closeLock] on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this@DefaultCatalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}