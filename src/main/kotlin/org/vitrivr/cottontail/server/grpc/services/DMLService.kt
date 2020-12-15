package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.locking.DeadlockException
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.database.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.DeleteImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.EntityScanImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.FilterImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.implementation.UpdateImplementationRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionStatus
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DMLGrpc
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.server.grpc.helper.GrpcQueryBinder
import org.vitrivr.cottontail.server.grpc.operators.SpoolerSinkOperator
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime
import kotlin.time.measureTimedValue

/**
 * Implementation of [DMLGrpc.DMLImplBase], the gRPC endpoint for inserting data into Cottontail DB [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
@ExperimentalTime
class DMLService(val catalogue: Catalogue, override val manager: TransactionManager) : DMLGrpc.DMLImplBase(), TransactionService {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(DMLService::class.java)
    }

    /** [GrpcQueryBinder] used to generate [org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression] tree from a gRPC query. */
    private val binder = GrpcQueryBinder(this.catalogue)

    /** [CottontailQueryPlanner] used to generate execution plans from query definitions. */
    private val planner = CottontailQueryPlanner(
        logicalRewriteRules = listOf(LeftConjunctionRewriteRule, RightConjunctionRewriteRule),
        physicalRewriteRules = listOf(BooleanIndexScanRule, EntityScanImplementationRule, FilterImplementationRule, DeleteImplementationRule, UpdateImplementationRule)
    )

    /**
     * gRPC endpoint for handling UPDATE queries.
     */
    override fun update(request: CottontailGrpc.UpdateMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        /* Determine query ID. */
        val queryId = request.queryId.ifBlank { UUID.randomUUID().toString() }

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                val bindTimedValue = measureTimedValue {
                    this.binder.parseAndBindUpdate(request, transaction)
                }
                LOGGER.trace("Parsing & binding UPDATE $queryId took ${bindTimedValue.duration}.")

                /* Plan query and create execution plan. */
                val plannedTimedValue = measureTimedValue {
                    val candidates = this.planner.plan(bindTimedValue.value)
                    if (candidates.isEmpty()) {
                        responseObserver.onError(Status.INTERNAL.withDescription("UPDATE query execution failed because no valid execution plan could be produced").asException())
                        return
                    }
                    SpoolerSinkOperator(candidates.minByOrNull { it.totalCost }!!.toOperator(this.manager), queryId, 0, responseObserver)
                }
                LOGGER.trace("Planning UPDATE $queryId took ${plannedTimedValue.duration}.")

                /* Execute query. */
                transaction.execute(plannedTimedValue.value)
            }

            /* Finalize transaction. */
            if (!commitDirectly && transaction.state === TransactionStatus.OPEN) {
                transaction.commit()
                LOGGER.trace("Executed UPDATE $queryId in $totalDuration.")
            } else {
                LOGGER.trace("Executed UPDATE $queryId in $totalDuration (pending commit).")
            }
            responseObserver.onCompleted()
        } catch (e: QueryException.QuerySyntaxException) {
            val message = "UPDATE query $queryId failed due to syntax error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            transaction.rollback()
        } catch (e: QueryException.QueryBindException) {
            val message = "UPDATE query $queryId failed due to binding error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "UPDATE query $queryId failed due to deadlock with other transaction: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: ExecutionException) {
            val message = "UPDATE query $queryId failed due to execution error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "UPDATE query $queryId failed due an unhandled error: ${e.message}"
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
            transaction.rollback()
        }
    }

    /**
     * gRPC endpoint for handling DELETE queries.
     */
    override fun delete(request: CottontailGrpc.DeleteMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        /* Determine query ID. */
        val queryId = request.queryId.ifBlank { UUID.randomUUID().toString() }

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                val bindTimedValue = measureTimedValue {
                    this.binder.parseAndBindDelete(request, transaction)
                }
                LOGGER.trace("Parsing & binding DELETE $queryId took ${bindTimedValue.duration}.")

                /* Plan query and create execution plan. */
                val planTimedValue = measureTimedValue {
                    val candidates = this.planner.plan(bindTimedValue.value)
                    if (candidates.isEmpty()) {
                        responseObserver.onError(Status.INTERNAL.withDescription("DELETE query execution failed because no valid execution plan could be produced").asException())
                        return
                    }
                    val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(this.manager)
                    SpoolerSinkOperator(operator, queryId, 0, responseObserver)
                }
                LOGGER.trace("Planning DELETE $queryId took ${planTimedValue.duration}.")

                /* Execute query. */
                transaction.execute(planTimedValue.value)
            }

            /* Finalize transaction. */
            if (!commitDirectly && transaction.state === TransactionStatus.OPEN) {
                transaction.commit()
                LOGGER.trace("Executed DELETE $queryId in $totalDuration.")
            } else {
                LOGGER.trace("Executed DELETE $queryId in $totalDuration (pending commit).")
            }
            responseObserver.onCompleted()
        } catch (e: QueryException.QuerySyntaxException) {
            val message = "DELETE query $queryId failed due to syntax error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            transaction.rollback()
        } catch (e: QueryException.QueryBindException) {
            val message = "DELETE query $queryId failed due to binding error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "DELETE query $queryId failed due to deadlock with other transaction: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: ExecutionException) {
            val message = "DELETE query $queryId failed due to execution error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "DELETE query $queryId failed due an unhandled error: ${e.message}"
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
            transaction.rollback()
        }
    }

    /**
     * gRPC endpoint for handling INSERT queries.
     */
    override fun insert(request: CottontailGrpc.InsertMessage, responseObserver: StreamObserver<CottontailGrpc.QueryResponseMessage>) {
        /* Determine query ID. */
        val queryId = request.queryId.ifBlank { UUID.randomUUID().toString() }

        /* Obtain transaction or create new one. */
        val commitDirectly = !request.hasTxId()
        val transaction = if (commitDirectly) {
            this.manager.Transaction()
        } else {
            this.resumeTransaction(request.txId, responseObserver) ?: return
        }

        try {
            val totalDuration = measureTime {
                /* Bind query and create logical plan. */
                val bindTimedValue = measureTimedValue {
                    this.binder.parseAndBindInsert(request, transaction)
                }
                LOGGER.info("Parsing & binding INSERT $queryId took ${bindTimedValue.duration}.")

                /* Plan query and create execution plan. */
                val planTimedValue = measureTimedValue {
                    val candidates = this.planner.plan(bindTimedValue.value)
                    if (candidates.isEmpty()) {
                        responseObserver.onError(Status.INTERNAL.withDescription("INSERT query execution failed because no valid execution plan could be produced").asException())
                        return
                    }
                    val operator = candidates.minByOrNull { it.totalCost }!!.toOperator(this.manager)
                    SpoolerSinkOperator(operator, queryId, 0, responseObserver)
                }
                LOGGER.info("Planning INSERT $queryId took ${planTimedValue.duration}.")

                /* Execute query. */
                transaction.execute(planTimedValue.value)
            }

            /* Finalize transaction. */
            if (!commitDirectly && transaction.state === TransactionStatus.OPEN) {
                transaction.commit()
                LOGGER.trace("Executed INSERT $queryId in $totalDuration.")
            } else {
                LOGGER.trace("Executed INSERT $queryId in $totalDuration (pending commit).")
            }
            responseObserver.onCompleted()
        } catch (e: QueryException.QuerySyntaxException) {
            val message = "INSERT query $queryId failed due to syntax error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            transaction.rollback()
        } catch (e: QueryException.QueryBindException) {
            val message = "INSERT query $queryId failed due to binding error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(message).asException())
            transaction.rollback()
        } catch (e: DeadlockException) {
            val message = "INSERT query $queryId failed due to deadlock with other transaction: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.ABORTED.withDescription(message).asException())
            transaction.rollback()
        } catch (e: ExecutionException) {
            val message = "INSERT query $queryId failed due to execution error: ${e.message}"
            LOGGER.info(message)
            responseObserver.onError(Status.INTERNAL.withDescription(message).asException())
            transaction.rollback()
        } catch (e: Throwable) {
            val message = "INSERT query $queryId failed due an unhandled error: ${e.message}"
            LOGGER.error(message, e)
            responseObserver.onError(Status.UNKNOWN.withDescription(message).asException())
            transaction.rollback()
        }
    }
}
