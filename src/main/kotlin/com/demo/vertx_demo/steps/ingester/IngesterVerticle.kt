package com.demo.vertx_demo.steps.ingester

import com.demo.vertx_demo.steps.APPLICATION_JSON
import com.demo.vertx_demo.steps.DEVICE_ID
import com.demo.vertx_demo.steps.DEVICE_SYNC
import com.demo.vertx_demo.steps.HTTP_PORT
import com.demo.vertx_demo.steps.INCOMING_STEPS_TOPIC
import com.demo.vertx_demo.steps.STEPS_COUNT
import com.demo.vertx_demo.steps.STEP_EVENTS_QUEUE
import com.demo.vertx_demo.steps.amqpConfig
import com.demo.vertx_demo.steps.producerConfig
import io.reactivex.Completable
import io.reactivex.Flowable
import io.vertx.amqp.AmqpReceiverOptions
import io.vertx.core.json.JsonObject
import io.vertx.reactivex.amqp.AmqpClient
import io.vertx.reactivex.amqp.AmqpMessage
import io.vertx.reactivex.amqp.AmqpReceiver
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.core.RxHelper
import io.vertx.reactivex.ext.web.Router
import io.vertx.reactivex.ext.web.RoutingContext
import io.vertx.reactivex.ext.web.handler.BodyHandler
import io.vertx.reactivex.kafka.client.producer.KafkaProducer
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class IngesterVerticle : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(IngesterVerticle::class.java)

    private lateinit var updateProducer: KafkaProducer<String, JsonObject>

    override fun rxStart(): Completable {
        updateProducer = KafkaProducer.create(vertx, producerConfig())
        val amqpOptions = amqpConfig()
        val amqpReceiverOptions = AmqpReceiverOptions().setAutoAcknowledgement(false).setDurable(true)

        AmqpClient.create(amqpOptions)
            .rxConnect()
            .doOnSuccess { logger.info("Successfully connected to ActiveMQ") }
            .flatMap { conn -> conn.rxCreateReceiver(STEP_EVENTS_QUEUE, amqpReceiverOptions) }
            .flatMapPublisher(AmqpReceiver::toFlowable)
            .doOnError(this::logAmqpError)
            .retryWhen(this::retryLater)
            .subscribe(this::handleAmqpMessage)

        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())
        router.post("/ingest")
            .handler(this::httpIngest)
            .failureHandler(this::handleFailedRouting)

        return vertx
            .createHttpServer()
            .exceptionHandler { err -> logger.error("HTTP server exception handler", err) }
            .requestHandler(router)
            .rxListen(config().getInteger(HTTP_PORT, 3002))
            .ignoreElement()
    }

    private fun httpIngest(ctx: RoutingContext) {
        val payload = ctx.bodyAsJson
        logger.info("HTTP ingest received: $payload")
        if (invalidIngestedJson(payload)) {
            logger.error("Invalid HTTP JSON (discarded): ${payload.encode()}")
            ctx.fail(400)
            return
        }
        val record = makeKafkaRecord(payload)
        updateProducer
            .rxSend(record)
            .subscribe(
                { ctx.response().end() },
                { err -> handleFailedHttpIngestion(err, ctx) }
            )
    }

    private fun handleFailedRouting(ctx: RoutingContext) {
        val statusCode = ctx.statusCode()
        logger.error("Routing failed, status code: $statusCode")
        ctx.response().setStatusCode(statusCode).end()
    }

    private fun handleFailedHttpIngestion(err: Throwable, ctx: RoutingContext) {
        logger.error("HTTP ingestion failed", err)
        ctx.fail(500)
    }

    private fun handleAmqpMessage(msg: AmqpMessage) {
        if (APPLICATION_JSON != msg.contentType() || invalidIngestedJson(msg.bodyAsJsonObject())) {
            logger.error("Invalid AMQP message (discarded): ${msg.bodyAsBinary()}")
            msg.accepted()
            return
        }
        val payload = msg.bodyAsJsonObject()
        val record = makeKafkaRecord(payload)
        updateProducer
            .rxSend(record)
            .subscribe(
                { msg.accepted() },
                { err -> handleFailedAmqpIngestion(err, msg) }
            )
    }

    private fun makeKafkaRecord(payload: JsonObject): KafkaProducerRecord<String, JsonObject> {
        val deviceId = payload.getString(DEVICE_ID)
        val recordData = JsonObject().put(DEVICE_ID, deviceId)
            .put(DEVICE_SYNC, payload.getLong(DEVICE_SYNC))
            .put(STEPS_COUNT, payload.getInteger(STEPS_COUNT))
        return KafkaProducerRecord.create(INCOMING_STEPS_TOPIC, deviceId, recordData)
    }

    private fun invalidIngestedJson(payload: JsonObject) =
        !payload.containsKey(DEVICE_ID) || !payload.containsKey(DEVICE_SYNC) || !payload.containsKey(STEPS_COUNT)

    private fun handleFailedAmqpIngestion(err: Throwable, msg: AmqpMessage) {
        logger.error("AMQP ingestion failed", err)
        msg.rejected()
    }

    private fun logAmqpError(err: Throwable) {
        logger.error("Woops AMQP", err)
    }

    private fun retryLater(errs: Flowable<Throwable>) =
        errs.delay(10L, TimeUnit.SECONDS, RxHelper.scheduler(vertx))
}