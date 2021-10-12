package com.demo.vertx_demo.steps.ingester

import com.demo.vertx_demo.steps.DEVICE_ID
import com.demo.vertx_demo.steps.DEVICE_SYNC
import com.demo.vertx_demo.steps.INCOMING_STEPS_TOPIC
import com.demo.vertx_demo.steps.STEPS_COUNT
import com.demo.vertx_demo.steps.STEP_EVENTS_QUEUE
import com.demo.vertx_demo.steps.amqpConfig
import io.restassured.RestAssured.given
import io.restassured.builder.RequestSpecBuilder
import io.restassured.filter.log.RequestLoggingFilter
import io.restassured.filter.log.ResponseLoggingFilter
import io.restassured.http.ContentType
import io.restassured.specification.RequestSpecification
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.reactivex.amqp.AmqpClient
import io.vertx.reactivex.amqp.AmqpMessage
import io.vertx.reactivex.core.RxHelper
import io.vertx.reactivex.core.Vertx
import io.vertx.reactivex.kafka.admin.KafkaAdminClient
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

@ExtendWith(VertxExtension::class)
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IngesterIntegrationTest {

    @Container
    private val containers: DockerComposeContainer<*> = DockerComposeContainer<Nothing>(File("docker-compose.yml"))

    private lateinit var kafkaConsumer: KafkaConsumer<String, JsonObject>
    private lateinit var amqpClient: AmqpClient
    private lateinit var requestSpecification: RequestSpecification

    @BeforeAll
    fun prepareSpec() {
        requestSpecification = RequestSpecBuilder()
            .addFilters(listOf(ResponseLoggingFilter(), RequestLoggingFilter()))
            .setBaseUri("http://localhost:3002/")
            .build()
    }

    @BeforeEach
    fun setUp(vertx: Vertx, testContext: VertxTestContext) {
        kafkaConsumer = KafkaConsumer.create(vertx, kafkaConfig())
        amqpClient = AmqpClient.create(vertx, amqpConfig())
        val kafkaAdminClient = KafkaAdminClient.create(vertx, kafkaConfig())
        vertx.rxDeployVerticle(IngesterVerticle())
            .delay(500L, TimeUnit.MILLISECONDS, RxHelper.scheduler(vertx))
            .flatMapCompletable { kafkaAdminClient.rxDeleteTopics(listOf(INCOMING_STEPS_TOPIC)) }
            .onErrorComplete()
            .subscribe(testContext::completeNow, testContext::failNow)
    }

    @Test
    @DisplayName("Ingest a well-formed AMQP message")
    fun amqpIngest(testContext: VertxTestContext) {
        val payload = JsonObject().put(DEVICE_ID, "123").put(DEVICE_SYNC, 1L).put(STEPS_COUNT, 500)

        amqpClient
            .rxConnect()
            .flatMap { conn -> conn.rxCreateSender(STEP_EVENTS_QUEUE) }
            .subscribe(
                { sender -> sender.send(amqpMessage(payload)) },
                { err -> testContext.failNow(err) }
            )

        verifyRecordSentToKafka(payload, testContext)
    }

    @Test
    @DisplayName("Ingest a badly-formed AMQP message and observe no Kafka record")
    fun amqpIngestWrong(vertx: Vertx, testContext: VertxTestContext) {
        val payload = JsonObject()

        amqpClient
            .rxConnect()
            .flatMap { conn -> conn.rxCreateSender(STEP_EVENTS_QUEUE) }
            .subscribe(
                { sender -> sender.send(amqpMessage(payload)) },
                { err -> testContext.failNow(err) }
            )

        verifyNoRecordSentToKafka(vertx, testContext)
    }

    @Test
    @DisplayName("Ingest a well-formed HTTP message")
    fun httpIngest(testContext: VertxTestContext) {
        val payload = JsonObject().put(DEVICE_ID, "123").put(DEVICE_SYNC, 1L).put(STEPS_COUNT, 500)

        given(requestSpecification)
            .contentType(ContentType.JSON)
            .body(payload.encode())
            .post("/ingest")
            .then()
            .assertThat()
            .statusCode(200)

        verifyRecordSentToKafka(payload, testContext)
    }

    @Test
    @DisplayName("Ingest a badly-formed JSON data over HTTP and observe no Kafka record")
    fun httpIngestWrong(vertx: Vertx, testContext: VertxTestContext) {
        val payload = JsonObject()

        given(requestSpecification)
            .contentType(ContentType.JSON)
            .body(payload.encode())
            .post("/ingest")
            .then()
            .assertThat()
            .statusCode(400)

        verifyNoRecordSentToKafka(vertx, testContext)
    }

    private fun verifyRecordSentToKafka(expectedRecord: JsonObject, testContext: VertxTestContext) {
        kafkaConsumer
            .subscribe(INCOMING_STEPS_TOPIC)
            .toFlowable()
            .subscribe(
                { record -> testContext.verify {
                    assertThat(record.key()).isEqualTo(expectedRecord.getString(DEVICE_ID))
                    val actualRecord = record.value()
                    assertThat(actualRecord.getString(DEVICE_ID)).isEqualTo(expectedRecord.getString(DEVICE_ID))
                    assertThat(actualRecord.getLong(DEVICE_SYNC)).isEqualTo(expectedRecord.getLong(DEVICE_SYNC))
                    assertThat(actualRecord.getInteger(STEPS_COUNT)).isEqualTo(expectedRecord.getInteger(STEPS_COUNT))
                    testContext.completeNow()
                }},
                { err -> testContext.failNow(err) }
            )
    }

    private fun verifyNoRecordSentToKafka(vertx: Vertx, testContext: VertxTestContext) {
        kafkaConsumer
            .subscribe(INCOMING_STEPS_TOPIC)
            .toFlowable()
            .timeout(3, TimeUnit.SECONDS, RxHelper.scheduler(vertx))
            .subscribe(
                { testContext.failNow(IllegalStateException("We must not get a record")) },
                { err -> if (err is TimeoutException) testContext.completeNow() else testContext.failNow(err) }
            )
    }

    private fun amqpMessage(payload: JsonObject) =
        AmqpMessage.create().durable(true).ttl(5000L).withJsonObjectAsBody(payload).build()

    private fun kafkaConfig() = mapOf(
        "bootstrap.servers" to "localhost:19092",
        "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" to "io.vertx.kafka.client.serialization.JsonObjectDeserializer",
        "auto.offset.reset" to "earliest",
        "enable.auto.commit" to "false",
        "group.id" to "ingester-test-${System.currentTimeMillis()}"
    )
}