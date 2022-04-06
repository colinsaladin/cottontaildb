package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Truncates an [Entity] (i.e., drops and re-creates it).
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class TruncateEntityOperator(private val tx: CatalogueTx, private val name: Name.EntityName) : AbstractDataDefinitionOperator(name, "TRUNCATE ENTITY") {
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemaTxn = context.getTx(this@TruncateEntityOperator.tx.schemaForName(this@TruncateEntityOperator.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this@TruncateEntityOperator.name)) as EntityTx
        val columns = entityTxn.listColumns().toTypedArray()
        val time = measureTimeMillis {
            schemaTxn.dropEntity(this@TruncateEntityOperator.name)
            schemaTxn.createEntity(this@TruncateEntityOperator.name, *columns)
        }
        emit(this@TruncateEntityOperator.statusRecord(time))
    }
}