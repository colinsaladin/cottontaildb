package org.vitrivr.cottontail

import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.grpc.DMLGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.model.values.IntVectorValue
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import java.nio.file.Files
import java.nio.file.Paths


object Playground {


    val channel = ManagedChannelBuilder.forAddress("127.0.0.1", 1865).usePlaintext().build()

    val dqlService = DQLGrpc.newBlockingStub(channel)
    val ddlService = DDLGrpc.newBlockingStub(channel)
    val dmlService = DMLGrpc.newBlockingStub(channel)

    val schema = CottontailGrpc.SchemaName.newBuilder().setName("cottontail").build()
    val entity = CottontailGrpc.EntityName.newBuilder()
            .setSchema(schema)
            .setName("tab4")
            .build()


    @JvmStatic
    fun main(args: Array<String>) {
        this.executeIn()
    }


    private fun executeUpdate() {
        val truncateMessage = CottontailGrpc.UpdateMessage.newBuilder()
                .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity))
                .setWhere(CottontailGrpc.Where.newBuilder().setCompound(
                        CottontailGrpc.CompoundBooleanPredicate.newBuilder()
                                .setAleft(CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                        .setLeft(CottontailGrpc.ColumnName.newBuilder().setName("objectid"))
                                        .setOp(CottontailGrpc.ComparisonOperator.EQUAL)
                                        .addRight(CottontailGrpc.Literal.newBuilder().setStringData("v_00071")))
                                .setAright(CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                        .setLeft(CottontailGrpc.ColumnName.newBuilder().setName("key"))
                                        .setOp(CottontailGrpc.ComparisonOperator.EQUAL)
                                        .addRight(CottontailGrpc.Literal.newBuilder().setStringData("duration")))

                ))
                .addUpdates(CottontailGrpc.UpdateMessage.UpdateElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("description")).setValue(CottontailGrpc.Literal.newBuilder().setStringData("this is a test!")))
        val results = this.dmlService.update(truncateMessage.build())
        println(TabulationUtilities.tabulate(results))
    }

    private fun executeDelete() {
        val truncateMessage = CottontailGrpc.DeleteMessage.newBuilder()
                .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity))
                .setWhere(CottontailGrpc.Where.newBuilder().setAtomic(
                        CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                .setLeft(CottontailGrpc.ColumnName.newBuilder().setName("objectid"))
                                .setOp(CottontailGrpc.ComparisonOperator.EQUAL)
                                .addRight(CottontailGrpc.Literal.newBuilder().setStringData("v_00093"))
                ))
        val results = this.dmlService.delete(truncateMessage.build())
        println(TabulationUtilities.tabulate(results))
    }


    private fun executeIn() {
        Files.newInputStream(Paths.get("/Users/rgasser/Downloads/query.proto")).use {
            val query = CottontailGrpc.QueryMessage.parseFrom(it)
            val results = this.dqlService.query(query)
            println(TabulationUtilities.tabulate(results))
        }
    }


    private fun executeKnn() {
        val vector = IntVectorValue.random(2).let {
            CottontailGrpc.Vector.newBuilder().setIntVector(CottontailGrpc.IntVector.newBuilder().addAllVector(it.data.asIterable()))
        }
        val query = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(CottontailGrpc.From.newBuilder().setEntity(entity))
                        .setKnn(CottontailGrpc.Knn.newBuilder()
                                .addQuery(vector)
                                .setDistance(CottontailGrpc.Knn.Distance.L2)
                                .setK(5)
                                .setAttribute(CottontailGrpc.ColumnName.newBuilder().setName("col27"))
                                .setHint(CottontailGrpc.KnnHint.newBuilder().setNoIndexHint(CottontailGrpc.KnnHint.NoIndexKnnHint.getDefaultInstance()))
                        )
                        .setProjection(CottontailGrpc.Projection.newBuilder()
                                .addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("col27")).setAlias(CottontailGrpc.ColumnName.newBuilder().setName("ctid")))
                                .addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("distance")).setAlias(CottontailGrpc.ColumnName.newBuilder().setName("dist")))
                        ))

        val results = this.dqlService.query(query.build())
        println(TabulationUtilities.tabulate(results))
    }
}