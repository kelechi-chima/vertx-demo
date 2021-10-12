package com.demo.vertx_demo.steps.congrats

import com.demo.vertx_demo.steps.DAILY_STEP_UPDATES_TOPIC
import com.demo.vertx_demo.steps.DEVICE_ID
import com.demo.vertx_demo.steps.INCOMING_STEPS_TOPIC
import com.demo.vertx_demo.steps.STEPS_COUNT
import com.demo.vertx_demo.steps.TIMESTAMP
import com.demo.vertx_demo.steps.producerConfig
import io.reactivex.Flowable
import io.vertx.core.json.JsonObject
import io.vertx.ext.mail.MailConfig
import io.vertx.ext.mail.MailMessage
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.reactivex.core.RxHelper
import io.vertx.reactivex.core.Vertx
import io.vertx.reactivex.ext.mail.MailClient
import io.vertx.reactivex.ext.web.client.HttpResponse
import io.vertx.reactivex.ext.web.client.WebClient
import io.vertx.reactivex.ext.web.codec.BodyCodec
import io.vertx.reactivex.kafka.admin.KafkaAdminClient
import io.vertx.reactivex.kafka.client.producer.KafkaProducer
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.File
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit.SECONDS

@ExtendWith(VertxExtension::class)
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CongratsIntegrationTest {
    private val logger = LoggerFactory.getLogger(CongratsIntegrationTest::class.java)

    @Container
    private val containers: DockerComposeContainer<*> = DockerComposeContainer<Nothing>(File("src/test/docker/congrats-docker-compose.yml"))

    private lateinit var kafkaProducer: KafkaProducer<String, JsonObject>
    private lateinit var webClient: WebClient

    @BeforeEach
    fun prepare(vertx: Vertx, testContext: VertxTestContext) {
        kafkaProducer = KafkaProducer.create(vertx, producerConfig())
        webClient = WebClient.create(vertx)
        val kafkaAdminClient = KafkaAdminClient.create(vertx, producerConfig())
        kafkaAdminClient
            .rxDeleteTopics(listOf(INCOMING_STEPS_TOPIC, DAILY_STEP_UPDATES_TOPIC))
            .onErrorComplete()
            .andThen(vertx.rxDeployVerticle(CongratsVerticle()))
            .ignoreElement()
            .andThen(vertx.rxDeployVerticle(FakeUserService()))
            .ignoreElement()
            .andThen(webClient.delete(8025, "localhost", "/api/va/messages").rxSend())
            .ignoreElement()
            .delay(1, SECONDS, RxHelper.scheduler(vertx))
            .subscribe(testContext::completeNow, testContext::failNow)
    }

    @Test
    @DisplayName("Smoke test to send a mail using mailhog")
    fun smokeTestSendmail(vertx: Vertx, testContext: VertxTestContext) {
        val config = MailConfig("localhost", 1025)
        val client = MailClient.createShared(vertx, config)
        val message = MailMessage("a@b.tld", "c@d.tld", "Yo", "This is cool")
        client
            .rxSendMail(message)
            .subscribe(
                { testContext.completeNow() },
                testContext::failNow
            )
    }

    @Test
    @DisplayName("No email must be sent below 10k steps")
    fun checkNothingBelow10k(vertx: Vertx, testContext: VertxTestContext) {
        kafkaProducer
            .rxSend(kafkaRecord("123", 5000))
            .ignoreElement()
            .delay(3, SECONDS, RxHelper.scheduler(vertx))
            .andThen(searchMail())
            .map(HttpResponse<JsonObject>::body)
            .subscribe(
                { json -> verifySentMail(0, json, testContext) },
                testContext::failNow
            )
    }

    @Test
    @DisplayName("An email must be sent for 10k steps")
    fun checkSendsOver10k(vertx: Vertx, testContext: VertxTestContext) {
        kafkaProducer
            .rxSend(kafkaRecord("123", 11_000))
            .ignoreElement()
            .delay(3, SECONDS, RxHelper.scheduler(vertx))
            .andThen(searchMail())
            .map(HttpResponse<JsonObject>::body)
            .subscribe(
                { json -> verifySentMail(1, json, testContext) },
                testContext::failNow
            )
    }

    @Test
    @DisplayName("Just one email must be sent to a user for 10k+ steps on single day")
    fun checkNotTwiceToday(vertx: Vertx, testContext: VertxTestContext) {
        kafkaProducer
            .rxSend(kafkaRecord("abc", 11_000))
            .ignoreElement()
            .delay(3, SECONDS, RxHelper.scheduler(vertx))
            .andThen(searchMail())
            .map(HttpResponse<JsonObject>::body)
            .map { json ->
                testContext.verify { assertThat(json.getInteger("total")).isEqualTo(1) }
                json
            }
            .ignoreElement()
            .andThen(kafkaProducer.rxSend(kafkaRecord("abc", 11_100)))
            .ignoreElement()
            .delay(3, SECONDS, RxHelper.scheduler(vertx))
            .andThen(searchMail())
            .map(HttpResponse<JsonObject>::body)
            .subscribe(
                { json -> verifySentMail(1, json, testContext) },
                testContext::failNow
            )
    }

    private fun searchMail() =
        webClient
            .get(8025, "localhost", "/api/v2/search?kind=to&query=foo@mail.tld")
            .`as`(BodyCodec.jsonObject())
            .rxSend()

    private fun verifySentMail(count: Int, json: JsonObject, testContext: VertxTestContext) {
        logger.info("Verifying email: $json")
        testContext.verify { assertThat(json.getInteger("total")).isEqualTo(count) }
        testContext.completeNow()
    }

    private fun kafkaRecord(deviceId: String, steps: Long): KafkaProducerRecord<String, JsonObject> {
        val now = LocalDateTime.now()
        val key = "$deviceId:${now.year}-${now.monthValue}-${now.dayOfMonth}"
        val recordData = JsonObject().put(DEVICE_ID, deviceId).put(TIMESTAMP, now.toString()).put(STEPS_COUNT, steps)
        return KafkaProducerRecord.create(DAILY_STEP_UPDATES_TOPIC, key, recordData)
    }

    private fun retryLater(vertx: Vertx, errs: Flowable<Throwable>) =
        errs.flatMap { Flowable.timer(10, SECONDS, RxHelper.scheduler(vertx)) }

    private fun kafkaConfig() = mapOf(
        "bootstrap.servers" to "localhost:19092",
        "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" to "io.vertx.kafka.client.serialization.JsonObjectDeserializer",
        "acks" to "1"
    )
}