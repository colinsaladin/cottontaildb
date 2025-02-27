package org.vitrivr.cottontail.dbms.column

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.DBO

/**
 * A [DBO] in the Cottontail DB data model that represents a [Column]. A [Column] can hold values
 * of a given type, as specified by the [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Column<T: Value> : DBO {

    /** The [Entity] this [Column] belongs to. */
    override val parent: Entity

    /** The [Name.ColumnName] of this [Column]. */
    override val name: Name.ColumnName

    /**
     * This [Column]'s [ColumnDef]. It contains all the relevant information that defines a [Column]
     *
     * @return [ColumnDef] for this [Column]
     */
    val columnDef: ColumnDef<T>

    /**
     * This [Column]'s [Types].
     *
     * @return The [Types] of this [Column].
     */
    val type: Types<T>
        get() = this.columnDef.type

    /**
     * Size of the content of this [Column]. The size is -1 (undefined) for most type of [Column]s.
     *
     * However, some column types like those holding arrays may have a defined size property
     */
    val size: Int
        get() = this.columnDef.type.logicalSize

    /**
     * Flag indicating whether this [Column] is nullable. Columns that are not nullable, cannot hold any null values.
     */
    val nullable: Boolean
        get() = this.columnDef.nullable

    /**
     * Creates a new [ColumnTx] for the given [TransactionContext].
     *
     * @param context [TransactionContext] to create [ColumnTx] for.
     */
    override fun newTx(context: TransactionContext): ColumnTx<T>
}