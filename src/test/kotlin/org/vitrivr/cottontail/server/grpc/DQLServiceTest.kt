package org.vitrivr.cottontail.server.grpc

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertTrue
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.TestConstants.STRING_COLUMN_NAME
import org.vitrivr.cottontail.TestConstants.TEST_ENTITY_FQN_INPUT
import org.vitrivr.cottontail.TestConstants.TEST_ENTITY_TUPLE_COUNT
import org.vitrivr.cottontail.TestConstants.TEST_VECTOR_ENTITY_FQN_INPUT
import org.vitrivr.cottontail.TestConstants.TWOD_COLUMN_NAME
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.extensions.Literal
import org.vitrivr.cottontail.client.stub.SimpleClient
import org.vitrivr.cottontail.embedded
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime

@ExperimentalTime
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DQLServiceTest {

    private lateinit var client: SimpleClient
    private lateinit var channel: ManagedChannel
    private lateinit var embedded: CottontailGrpcServer


    @BeforeAll
    fun startCottontail() {
        this.embedded = embedded(TestConstants.testConfig())
        val builder = NettyChannelBuilder.forAddress("localhost", 1865)
        builder.usePlaintext()
        this.channel = builder.build()
        this.client = SimpleClient(this.channel)
        assert(client.ping())
        dropTestSchema(client)
        createTestSchema(client)
        createTestVectorEntity(client)
        createTestEntity(client)
        populateTestEntity(client)
        populateVectorEntity(client)
    }

    @AfterAll
    fun cleanup() {
        dropTestSchema(client)

        /* Shutdown ManagedChannel. */
        this.channel.shutdown()
        this.channel.awaitTermination(5000, TimeUnit.MILLISECONDS)

        /* Stop embedded server. */
        this.embedded.stop()
    }

    @BeforeEach
    fun setup() {
        assert(client.ping())
    }

    @AfterEach
    fun tearDown() {
        assert(client.ping())
    }

    @Test
    fun pingTest() {
        assert(client.ping()) { "ping unsuccessful" }
    }

    @Test
    fun count() {
        val countQuery = Query(TEST_ENTITY_FQN_INPUT).count()
        val count = client.query(countQuery).next()
        assert(count.asLong(0)!! == TEST_ENTITY_TUPLE_COUNT)
    }

    @Test
    fun queryColumn() {
        val query = Query().from(TEST_ENTITY_FQN_INPUT).select(STRING_COLUMN_NAME)
        val result = client.query(query)
        assert(result.numberOfColumns == 1)
        val el = result.next()
        assert(!el.asString(STRING_COLUMN_NAME).equals(""))
    }

    @Test
    fun queryColumnWithVector() {
        val query = Query().from(TEST_VECTOR_ENTITY_FQN_INPUT).select(STRING_COLUMN_NAME)
        val result = client.query(query)
        assert(result.numberOfColumns == 1)
        val el = result.next()
        assert(!el.asString(STRING_COLUMN_NAME).equals(""))
    }

    @Test
    fun haversineDistance() {
        val query = Query().from(TEST_VECTOR_ENTITY_FQN_INPUT).knn(TWOD_COLUMN_NAME, 2, "haversine", arrayOf(5f, 10f))
        val result = client.query(query)
        val el = result.next()
        val distance = el.asDouble("distance")
        assert(distance != null)
    }

    @Test
    fun queryNNSWithLikeStart() {
        val query = Query().from(TEST_VECTOR_ENTITY_FQN_INPUT).knn(TWOD_COLUMN_NAME, 500, "l2", arrayOf(5f, 10f)).where(Literal(STRING_COLUMN_NAME, "LIKE", "a%"))
        val result = client.query(query)
        for (r in result) {
            val distance = r.asDouble("distance")
            val string = r.asString(STRING_COLUMN_NAME)!!
            assert(distance != null)
            assertTrue(string.first() == 'a')
        }
    }

    @Test
    fun queryNNSWithLikeEnd() {
        val query = Query().from(TEST_VECTOR_ENTITY_FQN_INPUT).knn(TWOD_COLUMN_NAME, 500, "l2", arrayOf(5f, 10f)).where(Literal(STRING_COLUMN_NAME, "LIKE", "%z"))
        val result = client.query(query)
        for (r in result) {
            val distance = r.asDouble("distance")
            val string = r.asString(STRING_COLUMN_NAME)!!
            assert(distance != null)
            assertTrue(string.last() == 'z')
        }
    }
}
