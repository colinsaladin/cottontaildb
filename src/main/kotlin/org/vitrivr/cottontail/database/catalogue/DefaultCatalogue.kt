package org.vitrivr.cottontail.database.catalogue

import jetbrains.exodus.env.forEach
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.entries.*
import org.vitrivr.cottontail.database.catalogue.entries.MetadataEntry.Companion.METADATA_ENTRY_DB_VERSION
import org.vitrivr.cottontail.database.general.*
import org.vitrivr.cottontail.database.lmdb.executeInWriteTransaction
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.functions.FunctionRegistry
import org.vitrivr.cottontail.functions.initialize
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.locks.StampedLock
import kotlin.concurrent.withLock

/**
 * The default [Catalogue] implementation based on JetBrains Xodus.
 *
 * @see Catalogue
 * @see CatalogueTx
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultCatalogue(override val config: Config) : Catalogue {
    /**
     * Companion object to [DefaultCatalogue]
     */
    companion object {
        /** Prefix used for actual column stores. */
        internal const val ENTITY_STORE_PREFIX: String = "ctt_ent"

        /** Prefix used for actual column stores. */
        internal const val COLUMN_STORE_PREFIX: String = "ctt_col"

        /** Prefix used for actual index stores. */
        internal const val INDEX_STORE_PREFIX: String = "ctt_idx"
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

    /** The [FunctionRegistry] exposed by this [Catalogue]. */
    override val functions: FunctionRegistry = FunctionRegistry()

    /** Status indicating whether this [DefaultCatalogue] is open or closed. */
    override val closed: Boolean
        get() = this.environment.isClosed

    /** A lock used to mediate access to this [DefaultCatalogue]. This is an internal variable and not part of the official interface. */
    internal val closeLock = StampedLock()

    /** The Xodus environment used for Cottontail DB. This is an internal variable and not part of the official interface. */
    internal val environment: Env<ByteBuffer> = Env.create()
        .setMapSize(Long.MAX_VALUE)
        .setMaxDbs(Int.MAX_VALUE)
        .open(this.config.root.resolve("cottontail.db").toFile(), EnvFlags.MDB_NOSUBDIR)

    init {
        /* Check if catalogue has been initialized and initialize if needed. */
        this.environment.executeInWriteTransaction { tx ->
            if (this.environment.dbiNames.size == 0) {
                /* Initialize database metadata. */
                MetadataEntry.init(this, tx)
                MetadataEntry.write(MetadataEntry(METADATA_ENTRY_DB_VERSION, this.version.toString()), this, tx)

                /* Initialize necessary stores. */
                SchemaCatalogueEntry.init(this, tx)
                EntityCatalogueEntry.init(this, tx)
                SequenceCatalogueEntries.init(this, tx)
                ColumnCatalogueEntry.init(this, tx)
                StatisticsCatalogueEntry.init(this, tx)
                IndexCatalogueEntry.init(this, tx)
                IndexStructCatalogueEntry.init(this, tx)
            }

            /* Check database version. */
            val version = MetadataEntry.read(METADATA_ENTRY_DB_VERSION, this, tx)?.let { it -> DBOVersion.valueOf(it.value) } ?: DBOVersion.UNDEFINED
            if (version != this.version) {
                throw DatabaseException.VersionMismatchException(this.version, version)
            }
        }

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

        /**
         * Obtains a global (non-exclusive) read-lock on [DefaultCatalogue].
         *
         * Prevents [DefaultCatalogue] from being closed while transaction is ongoing.
         */
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
        override fun listSchemas(): List<Name.SchemaName> = this.txLatch.withLock {
            val store = SchemaCatalogueEntry.store(this@DefaultCatalogue, this.context.xodusTx)
            val list = mutableListOf<Name.SchemaName>()
            store.openCursor(this.context.xodusTx).forEach {
                val entry = SchemaCatalogueEntry.entryToObject(this.value) as SchemaCatalogueEntry
                list.add(entry.name)
            }
            return list
        }

        /**
         * Returns the [Schema] for the given [Name.SchemaName].
         *
         * @param name [Name.SchemaName] to obtain the [Schema] for.
         */
        override fun schemaForName(name: Name.SchemaName): Schema = this.txLatch.withLock {
            if (!SchemaCatalogueEntry.exists(name, this@DefaultCatalogue, this.context.xodusTx)) {
                throw DatabaseException.SchemaDoesNotExistException(name)
            }
            return DefaultSchema(name, this@DefaultCatalogue)
        }

        /**
         * Creates a new, empty [Schema] with the given [Name.SchemaName] and [Path]
         *
         * @param name The [Name.SchemaName] of the new [Schema].
         */
        override fun createSchema(name: Name.SchemaName): Schema = this.txLatch.withLock {
            /* Check if schema exists! */
            if (SchemaCatalogueEntry.exists(name, this@DefaultCatalogue, this.context.xodusTx)) throw DatabaseException.SchemaAlreadyExistsException(name)

            /* Write schema! */
            SchemaCatalogueEntry.write(SchemaCatalogueEntry(name), this@DefaultCatalogue, this.context.xodusTx)
            return DefaultSchema(name, this@DefaultCatalogue)
        }

        /**
         * Drops an existing [Schema] with the given [Name.SchemaName].
         *
         * @param name The [Name.SchemaName] of the [Schema] to be dropped.
         */
        override fun dropSchema(name: Name.SchemaName) = this.txLatch.withLock {
            /* Check if schema exists! */
            if (!SchemaCatalogueEntry.exists(name, this@DefaultCatalogue, this.context.xodusTx)) {
                throw DatabaseException.SchemaDoesNotExistException(name)
            }

            /* Drop all entities from schema. */
            val schemaTx = DefaultSchema(name, this@DefaultCatalogue).newTx(this.context)
            schemaTx.listEntities().forEach { schemaTx.dropEntity(it) }

            /* Remove schema from catalogue. */
            SchemaCatalogueEntry.delete(name, this@DefaultCatalogue, this.context.xodusTx)
            Unit
        }

        /**
         * Called when a transaction finalizes. Releases the lock held on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}