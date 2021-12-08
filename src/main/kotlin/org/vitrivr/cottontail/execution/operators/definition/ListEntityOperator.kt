package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.binding.EmptyBindingContext
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.SourceOperator] used during query execution. Lists all available [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ListEntityOperator(val catalogue: DefaultCatalogue, val schema: Name.SchemaName? = null) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName(Constants.COLUMN_NAME_DBO), Type.String, false),
            ColumnDef(Name.ColumnName(Constants.COLUMN_NAME_CLASS), Type.String, false)
        )
    }

    /** The [BindingContext] used [AbstractDataDefinitionOperator]. */
    override val binding: BindingContext = EmptyBindingContext

    override val columns: List<ColumnDef<*>> = COLUMNS

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val txn = context.getTx(this.catalogue) as CatalogueTx
        val schemas = if (this.schema != null) {
            listOf(txn.schemaForName(this.schema))
        } else {
            txn.listSchemas()
        }
        val columns = this.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(columns.size)
        values[1] = StringValue("ENTITY")
        return flow {
            var i = 0L
            for (schema in schemas) {
                val schemaTxn = context.getTx(schema) as SchemaTx
                for (entity in schemaTxn.listEntities()) {
                    values[0] = StringValue(entity.name.toString())
                    emit(StandaloneRecord(i++, columns, values))
                }
            }
        }
    }
}