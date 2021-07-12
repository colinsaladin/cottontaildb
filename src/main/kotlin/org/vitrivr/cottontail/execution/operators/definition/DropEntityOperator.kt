package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Drops an [Entity]
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
class DropEntityOperator(val catalogue: DefaultCatalogue, val name: Name.EntityName) : AbstractDataDefinitionOperator(name, "DROP ENTITY") {
    override fun toFlow(context: QueryContext): Flow<Record> {
        val catTxn = context.txn.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.txn.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        return flow {
            val timedTupleId = measureTimedValue {
                schemaTxn.dropEntity(this@DropEntityOperator.name)
            }
            emit(this@DropEntityOperator.statusRecord(timedTupleId.duration))
        }
    }
}