package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Truncates an [Entity] (i.e. drops and re-creates it).
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
@ExperimentalTime
class TruncateEntityOperator(val catalogue: Catalogue, val name: Name.EntityName) : AbstractDataDefinitionOperator(name, "TRUNCATE ENTITY") {
    override fun toFlow(context: QueryContext): Flow<Record> {
        val catTxn = context.txn.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.txn.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.txn.getTx(schemaTxn.entityForName(this.name)) as EntityTx
        return flow {
            val columns =
                entityTxn.listColumns().map { Pair(it.columnDef, it.engine) }.toTypedArray()
            val timedTupleId = measureTimedValue {
                schemaTxn.dropEntity(this@TruncateEntityOperator.name)
                schemaTxn.createEntity(this@TruncateEntityOperator.name, *columns)
            }
            emit(this@TruncateEntityOperator.statusRecord(timedTupleId.duration))
        }
    }
}