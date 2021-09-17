package org.vitrivr.cottontail.server.grpc.services

import kotlinx.coroutines.flow.single
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.GrpcQueryBinder
import org.vitrivr.cottontail.database.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.database.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.database.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DMLGrpc
import org.vitrivr.cottontail.grpc.DMLGrpcKt
import kotlin.time.ExperimentalTime

/**
 * Implementation of [DMLGrpc.DMLImplBase], the gRPC endpoint for inserting data into Cottontail DB [DefaultEntity]s.
 *
 * @author Ralph Gasser
 * @version 2.0.2
 */
@ExperimentalTime
class DMLService(val catalogue: Catalogue, override val manager: TransactionManager) : DMLGrpcKt.DMLCoroutineImplBase(), gRPCTransactionService {

    /** [CottontailQueryPlanner] instance used to generate execution plans from query definitions. */
    private val planner = CottontailQueryPlanner(
        logicalRules = listOf(LeftConjunctionRewriteRule, RightConjunctionRewriteRule),
        physicalRules = listOf(BooleanIndexScanRule),
        this.catalogue.config.cache.planCacheSize
    )

    /**
     * gRPC endpoint for handling UPDATE queries.
     */
    override suspend fun update(request: CottontailGrpc.UpdateMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "UPDATE", false) { tx, q ->
        val ctx = QueryContext(this.catalogue, tx)

        /* Bind query and create logical plan. */
        GrpcQueryBinder.bind(request, ctx)

        /* Generate physical execution plan for query. */
        this.planner.planAndSelect(ctx)

        /* Execute UPDATE. */
        executeAndMaterialize(ctx, ctx.toOperatorTree(), q, 0)
    }.single()

    /**
     * gRPC endpoint for handling DELETE queries.
     */
    override suspend fun delete(request: CottontailGrpc.DeleteMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "DELETE", false) { tx, q ->
        val ctx = QueryContext(this.catalogue, tx)

        /* Bind query and create logical plan. */
        GrpcQueryBinder.bind(request, ctx)

        /* Generate physical execution plan for query. */
        this.planner.planAndSelect(ctx)

        /* Execute DELETE. */
        executeAndMaterialize(ctx, ctx.toOperatorTree(), q, 0)
    }.single()

    /**
     * gRPC endpoint for handling INSERT queries.
     */
    override suspend fun insert(request: CottontailGrpc.InsertMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "INSERT", false) { tx, q ->
        val ctx = QueryContext(this.catalogue, tx)

        /* Bind query and create logical + physical plan (bypass query planner). */
        GrpcQueryBinder.bind(request, ctx)
        ctx.physical = ctx.logical?.implement()

        /* Execute INSERT. */
        executeAndMaterialize(ctx, ctx.toOperatorTree(), q, 0)
    }.single()

    /**
     * gRPC endpoint for handling INSERT BATCH queries.
     */
    override suspend fun insertBatch(request: CottontailGrpc.BatchInsertMessage): CottontailGrpc.QueryResponseMessage = this.withTransactionContext(request.txId, "INSERT BATCH", false) { tx, q ->
        val ctx = QueryContext(this.catalogue, tx)

        /* Bind query and create logical plan. */
        GrpcQueryBinder.bind(request, ctx)
        ctx.physical = ctx.logical?.implement()

        /* Execute INSERT. */
        executeAndMaterialize(ctx, ctx.toOperatorTree(), q, 0)
    }.single()
}
