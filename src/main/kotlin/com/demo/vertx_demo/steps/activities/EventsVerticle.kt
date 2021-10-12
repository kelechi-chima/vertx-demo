package com.demo.vertx_demo.steps.activities

import com.demo.vertx_demo.steps.DAILY_STEP_UPDATES_TOPIC
import com.demo.vertx_demo.steps.DEVICE_ID
import com.demo.vertx_demo.steps.DEVICE_SYNC
import com.demo.vertx_demo.steps.INCOMING_STEPS_TOPIC
import com.demo.vertx_demo.steps.INSERT_STEP_EVENT_QUERY
import com.demo.vertx_demo.steps.PG_DUPLICATE_KEY_ERROR_CODE
import com.demo.vertx_demo.steps.STEPS_COUNT
import com.demo.vertx_demo.steps.TIMESTAMP
import com.demo.vertx_demo.steps.TODAYS_STEP_COUNT_QUERY
import com.demo.vertx_demo.steps.consumerConfig
import com.demo.vertx_demo.steps.pgConnectOptions
import com.demo.vertx_demo.steps.producerConfig
import io.reactivex.Completable
import io.reactivex.Flowable
import io.vertx.core.json.JsonObject
import io.vertx.pgclient.PgException
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.core.RxHelper
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord
import io.vertx.reactivex.kafka.client.producer.KafkaProducer
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord
import io.vertx.reactivex.pgclient.PgPool
import io.vertx.reactivex.sqlclient.Tuple
import io.vertx.sqlclient.PoolOptions
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

class EventsVerticle : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(EventsVerticle::class.java)

    private lateinit var eventConsumer: KafkaConsumer<String, JsonObject>
    private lateinit var updateProducer: KafkaProducer<String, JsonObject>
    private lateinit var pgPool: PgPool

    override fun rxStart(): Completable {
        eventConsumer = KafkaConsumer.create(vertx, consumerConfig("activity-service"))
        updateProducer = KafkaProducer.create(vertx, producerConfig())
        pgPool = PgPool.pool(vertx, pgConnectOptions(), PoolOptions())
        eventConsumer
            .subscribe(INCOMING_STEPS_TOPIC)
            .toFlowable()
            .flatMap(this::insertRecord)
            .flatMap(this::generateActivityUpdate)
            .flatMap(this::commitKafkaConsumerOffset)
            .doOnError { err -> logger.info("Woops", err) }
            .retryWhen(this::retryLater)
            .subscribe()

        return Completable.complete()
    }

    private fun insertRecord(record: KafkaConsumerRecord<String, JsonObject>): Flowable<KafkaConsumerRecord<String, JsonObject>> {
        val data = record.value()
        val values = Tuple.of(
            data.getString(DEVICE_ID),
            data.getLong(DEVICE_SYNC),
            data.getInteger(STEPS_COUNT)
        )
        return pgPool
            .preparedQuery(INSERT_STEP_EVENT_QUERY)
            .rxExecute(values)
            .map { record }
            .onErrorReturn { err ->
                if (duplicateKeyInsert(err)) {
                    record
                } else {
                    throw RuntimeException(err)
                }
            }
            .toFlowable()
    }

    private fun generateActivityUpdate(record: KafkaConsumerRecord<String, JsonObject>): Flowable<KafkaConsumerRecord<String, JsonObject>> {
        val deviceId = record.value().getString(DEVICE_ID)
        val now = LocalDateTime.now()
        val key = "$deviceId:${now.year}-${now.monthValue}-${now.dayOfMonth}"
        return pgPool
            .preparedQuery(TODAYS_STEP_COUNT_QUERY)
            .rxExecute(Tuple.of(deviceId))
            .map { rs -> rs.iterator().next() }
            .map { row ->
                JsonObject()
                    .put(DEVICE_ID, deviceId)
                    .put(TIMESTAMP, row.getTemporal(0).toString())
                    .put(STEPS_COUNT, row.getLong(1))
            }
            .flatMap { recordData ->
                updateProducer.rxSend(KafkaProducerRecord.create(DAILY_STEP_UPDATES_TOPIC, key, recordData))
            }
            .map { record }
            .toFlowable()
    }

    private fun commitKafkaConsumerOffset(record: KafkaConsumerRecord<String, JsonObject>): Publisher<*> {
        return eventConsumer.rxCommit().toFlowable<Any>()
    }

    private fun retryLater(err: Flowable<Throwable>): Flowable<Throwable>? {
        return err.delay(10, TimeUnit.SECONDS, RxHelper.scheduler(vertx))
    }

    private fun duplicateKeyInsert(err: Throwable) = err is PgException && PG_DUPLICATE_KEY_ERROR_CODE == err.code
}