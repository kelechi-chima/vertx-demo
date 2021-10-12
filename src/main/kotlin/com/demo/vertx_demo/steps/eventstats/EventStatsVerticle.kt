package com.demo.vertx_demo.steps.eventstats

import com.demo.vertx_demo.steps.CITY
import com.demo.vertx_demo.steps.DAILY_STEP_UPDATES_TOPIC
import com.demo.vertx_demo.steps.DEVICE_ID
import com.demo.vertx_demo.steps.EVENT_STATS_CITY_TREND_UPDATES_TOPIC
import com.demo.vertx_demo.steps.EVENT_STATS_THROUGHPUT_TOPIC
import com.demo.vertx_demo.steps.EVENT_STATS_USER_ACTIVITY_UPDATES_TOPIC
import com.demo.vertx_demo.steps.INCOMING_STEPS_TOPIC
import com.demo.vertx_demo.steps.STEPS_COUNT
import com.demo.vertx_demo.steps.TIMESTAMP
import com.demo.vertx_demo.steps.USERNAME
import com.demo.vertx_demo.steps.consumerConfig
import com.demo.vertx_demo.steps.producerConfig
import io.reactivex.Completable
import io.reactivex.CompletableSource
import io.reactivex.Flowable
import io.reactivex.Single
import io.vertx.core.json.JsonObject
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.core.RxHelper
import io.vertx.reactivex.ext.web.client.HttpResponse
import io.vertx.reactivex.ext.web.client.WebClient
import io.vertx.reactivex.ext.web.codec.BodyCodec
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord
import io.vertx.reactivex.kafka.client.producer.KafkaProducer
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit.SECONDS

class EventStatsVerticle : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(EventStatsVerticle::class.java)

    private lateinit var webClient: WebClient
    private lateinit var producer: KafkaProducer<String, JsonObject>

    override fun rxStart(): Completable {
        webClient = WebClient.create(vertx)
        producer = KafkaProducer.create(vertx, producerConfig())

        KafkaConsumer.create<String, JsonObject>(vertx, consumerConfig("event-stats-user-activity-updates"))
            .subscribe(DAILY_STEP_UPDATES_TOPIC)
            .toFlowable()
            .flatMapSingle(this::addDeviceOwner)
            .flatMapSingle(this::addOwnerData)
            .flatMapCompletable(this::publishUserActivityUpdate)
            .doOnError { err -> logger.error("Woops", err) }
            .retryWhen(this::retryLater)
            .subscribe()

        KafkaConsumer.create<String, JsonObject>(vertx, consumerConfig("event-stats-throughput"))
            .subscribe(INCOMING_STEPS_TOPIC)
            .toFlowable()
            .buffer(5L, SECONDS, RxHelper.scheduler(vertx))
            .flatMapCompletable(this::publishThroughput)
            .doOnError { err -> logger.error("Woops", err) }
            .retryWhen(this::retryLater)
            .subscribe()

        KafkaConsumer.create<String, JsonObject>(vertx, consumerConfig("event-stats-city-trends"))
            .subscribe(EVENT_STATS_USER_ACTIVITY_UPDATES_TOPIC)
            .toFlowable()
            .groupBy(this::city)
            .flatMap { group -> group.buffer(5, SECONDS, RxHelper.scheduler(vertx)) }
            .flatMapCompletable(this::publishCityTrendUpdate)
            .doOnError { err -> logger.error("Woops", err) }
            .retryWhen(this::retryLater)
            .subscribe()

        return Completable.complete()
    }

    private fun addDeviceOwner(record: KafkaConsumerRecord<String, JsonObject>): Single<JsonObject> {
        val recordData = record.value()
        return webClient
            .get(3000, "localhost", "/owns/${recordData.getString(DEVICE_ID)}")
            .`as`(BodyCodec.jsonObject())
            .rxSend()
            .map(HttpResponse<JsonObject>::body)
            .map(recordData::mergeIn)
    }

    private fun addOwnerData(data: JsonObject): Single<JsonObject> {
        val username = data.getString(USERNAME)
        return webClient
            .get(3000, "localhost", "/$username")
            .`as`(BodyCodec.jsonObject())
            .rxSend()
            .map(HttpResponse<JsonObject>::body)
            .map(data::mergeIn)
    }

    private fun publishUserActivityUpdate(data: JsonObject): CompletableSource {
        val record = KafkaProducerRecord.create(EVENT_STATS_USER_ACTIVITY_UPDATES_TOPIC, data.getString(USERNAME), data)
        return producer.rxWrite(record)
    }

    private fun publishThroughput(records: List<KafkaConsumerRecord<String, JsonObject>>): CompletableSource {
        val recordData = JsonObject()
            .put("seconds", 5)
            .put("count", records.size)
            .put("throughput", records.size / 5.0)
        val record = KafkaProducerRecord.create<String, JsonObject>(EVENT_STATS_THROUGHPUT_TOPIC, recordData)
        return producer.rxWrite(record)
    }

    private fun publishCityTrendUpdate(records: List<KafkaConsumerRecord<String, JsonObject>>): Completable {
        return if (records.isNotEmpty()) {
            val city = city(records[0])
            val stepsCount = records.sumOf { record -> record.value().getLong(STEPS_COUNT) }
            val recordData = JsonObject()
                .put(TIMESTAMP, LocalDateTime.now().toString())
                .put(STEPS_COUNT, stepsCount)
                .put(CITY, city)
                .put("seconds", 5)
                .put("updates", records.size)
            val record = KafkaProducerRecord.create(EVENT_STATS_CITY_TREND_UPDATES_TOPIC, city, recordData)
            producer.rxWrite(record)
        } else {
            Completable.complete()
        }
    }

    private fun city(record: KafkaConsumerRecord<String, JsonObject>) = record.value().getString(CITY)

    private fun retryLater(err: Flowable<Throwable>): Flowable<Throwable>? {
        return err.delay(10L, SECONDS, RxHelper.scheduler(vertx))
    }
}