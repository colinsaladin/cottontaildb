package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * An [Operator.SourceOperator] used during query execution. Lists all available [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ListEntityOperator(val catalogue: DefaultCatalogue, val schema: Name.SchemaName? = null) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName(Constants.COLUMN_NAME_DBO), Types.String, false),
            ColumnDef(Name.ColumnName(Constants.COLUMN_NAME_CLASS), Types.String, false)
        )
    }

    override val columns: List<ColumnDef<*>> = COLUMNS

    override fun toFlow(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): Flow<Record> {
        val txn = context.getTx(this.catalogue) as CatalogueTx
        val schemas = if (this.schema != null) {
            listOf(txn.schemaForName(this.schema))
        } else {
            txn.listSchemas().map { txn.schemaForName(it) }
        }
        val columns = this.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(columns.size)
        values[1] = StringValue("ENTITY")
        return flow {
            var i = 0L
            for (schema in schemas) {
                val schemaTxn = context.getTx(schema) as SchemaTx
                for (entity in schemaTxn.listEntities()) {
                    values[0] = StringValue(entity.toString())
                    emit(StandaloneRecord(i++, columns, values))
                }
            }
        }
    }
}