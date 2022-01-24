package org.vitrivr.cottontail.execution.operators.function

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.model.basics.Record

/**
 * A [Operator.PipelineOperator] used during query execution. It executes a defined [Function] and generates [Binding] from its result.
 *
 * Used to execute nested functions whose result is not manifested in a [Record]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class NestedFunctionProjectionOperator(parent: Operator, val function: Function<*>, val arguments: List<Binding>, override val binding: BindingContext, val out: Binding.Literal) : Operator.PipelineOperator(parent) {

    /** The [DistanceProjectionOperator] is not a pipeline breaker. */
    override val breaker: Boolean = false
    override val columns: List<ColumnDef<*>>
        get() = parent.columns

    /**
     * Converts this [FunctionProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution
     * @return [Flow] representing this [FunctionProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        /* Obtain parent flow. */
        val parentFlow = this.parent.toFlow(context)

        /* Prepare arguments for function. */
        val dynamicArgs = mutableListOf<Pair<Int, Binding>>()
        this.arguments.forEachIndexed {  i, b ->
            if (b.static) {
                this.function.provide(i, b.value)
            } else {
                dynamicArgs.add(i to b)
            }
        }

        /* Prepare empty array that acts a holder for values. */
        return parentFlow.onEach { _ ->
            dynamicArgs.forEach { this@NestedFunctionProjectionOperator.function.provide(it.first, it.second.value) }
            this@NestedFunctionProjectionOperator.binding.update(this@NestedFunctionProjectionOperator.out, this@NestedFunctionProjectionOperator.function())
        }
    }
}