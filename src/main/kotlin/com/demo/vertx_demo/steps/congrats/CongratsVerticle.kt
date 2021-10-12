package com.demo.vertx_demo.steps.congrats

import com.demo.vertx_demo.steps.DAILY_STEP_UPDATES_TOPIC
import com.demo.vertx_demo.steps.DEVICE_ID
import com.demo.vertx_demo.steps.EMAIL
import com.demo.vertx_demo.steps.STEPS_COUNT
import com.demo.vertx_demo.steps.USERNAME
import com.demo.vertx_demo.steps.consumerConfig
import com.demo.vertx_demo.steps.mailConfig
import io.reactivex.Completable
import io.reactivex.Flowable
import io.reactivex.Single
import io.vertx.core.json.JsonObject
import io.vertx.ext.mail.MailMessage
import io.vertx.ext.mail.MailResult
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.core.RxHelper
import io.vertx.reactivex.ext.mail.MailClient
import io.vertx.reactivex.ext.web.client.HttpResponse
import io.vertx.reactivex.ext.web.client.WebClient
import io.vertx.reactivex.ext.web.codec.BodyCodec
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

class CongratsVerticle : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(CongratsVerticle::class.java)

    private lateinit var webClient: WebClient
    private lateinit var mailClient: MailClient

    override fun rxStart(): Completable {
        webClient = WebClient.create(vertx)
        mailClient = MailClient.createShared(vertx, mailConfig())

        KafkaConsumer.create<String, JsonObject>(vertx, consumerConfig("congrats-service"))
            .subscribe(DAILY_STEP_UPDATES_TOPIC)
            .toFlowable()
            .filter(this::above10k)
            .distinct(KafkaConsumerRecord<String, JsonObject>::key)
            .flatMapSingle(this::sendMail)
            .doOnError { err -> logger.error("Woops", err) }
            .retryWhen(this::retryLater)
            .subscribe { mailResult -> logger.info("Congratulated ${mailResult.recipients}") }

        return Completable.complete()
    }

    private fun above10k(record: KafkaConsumerRecord<String, JsonObject>) =
        record.value().getInteger(STEPS_COUNT) >= 10_000

    private fun sendMail(record: KafkaConsumerRecord<String, JsonObject>): Single<MailResult> {
        val deviceId = record.value().getString(DEVICE_ID)
        val stepsCount = record.value().getInteger(STEPS_COUNT)
        return getUser(deviceId)
            .flatMap { user -> getEmail(user) }
            .doOnSuccess { email -> logger.info("About to send email to $email")  }
            .map { email -> makeEmail(stepsCount, email) }
            .flatMap(mailClient::rxSendMail)
    }

    private fun getUser(deviceId: String): Single<String> =
        webClient
            .get(3000, "localhost", "/owns/$deviceId")
            .`as`(BodyCodec.jsonObject())
            .rxSend()
            .map(HttpResponse<JsonObject>::body)
            .map { json -> json.getString(USERNAME) }

    private fun getEmail(username: String): Single<String> =
        webClient
            .get(3000, "localhost","/$username")
            .`as`(BodyCodec.jsonObject())
            .rxSend()
            .map(HttpResponse<JsonObject>::body)
            .map { json -> json.getString(EMAIL) }

    private fun makeEmail(stepsCount: Int, email: String) = MailMessage().apply {
        from = "noreply@ten10ksteps.tld"
        setTo(email)
        subject = "You made it!"
        text = "Congratulations on reaching $stepsCount steps today!\n\n- The 10k Steps Team\n"
    }

    private fun retryLater(errs: Flowable<Throwable>) =
        errs.delay(10L, TimeUnit.SECONDS, RxHelper.scheduler(vertx))
}