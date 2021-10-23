package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.ExecutionException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.PipelineOperator] used during query execution. It calculates the MEAN of all values
 * it has encountered and returns it as a [Record].
 *
 * Only produces a single [Record] and converts the projected columns to a [Type.Double] column.
 * Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class MeanProjectionOperator(parent: Operator, fields: List<Name.ColumnName>) : Operator.PipelineOperator(parent) {

    /** [MaxProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MeanProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = this.parent.columns.mapNotNull { c ->
        val match = fields.find { f -> f.matches(c.name) }
        if (match != null) {
            if (!c.type.numeric) throw OperatorSetupException(this, "The provided column $match cannot be used for a ${Projection.SUM} projection because it has the wrong type.")
            ColumnDef(c.name, Type.Double)
        } else {
            null
        }
    }

    /** Parent [ColumnDef] to access and aggregate. */
    private val parentColumns = this.parent.columns.filter { c -> fields.any { f -> f.matches(c.name) } }

    /**
     * Converts this [MeanProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution
     * @return [Flow] representing this [MeanProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        val columns = this.columns.toTypedArray()
        return flow {
            /* Prepare holder of type double. */
            val count = this@MeanProjectionOperator.parentColumns.map { 0L }.toTypedArray()
            val sum = this@MeanProjectionOperator.parentColumns.map { 0.0 }.toTypedArray()
            parentFlow.onEach {
                this@MeanProjectionOperator.parentColumns.forEachIndexed { i, c ->
                    val value = it[c]
                    if (value != null) {
                        count[i] += 1L
                        sum[i] += when (value) {
                            is ByteValue -> value.value.toDouble()
                            is ShortValue -> value.value.toDouble()
                            is IntValue -> value.value.toDouble()
                            is LongValue -> value.value.toDouble()
                            is FloatValue -> value.value.toDouble()
                            is DoubleValue -> value.value
                            null -> 0.0
                            else -> throw ExecutionException.OperatorExecutionException(this@MeanProjectionOperator, "The provided column $c cannot be used for a ${Projection.MEAN} projection.")
                        }
                    }
                }
            }.collect()

            /** Emit record. */
            val results = Array<Value?>(sum.size) { DoubleValue(sum[it] / count[it]) }
            emit(StandaloneRecord(0L, columns, results))
        }
    }
}