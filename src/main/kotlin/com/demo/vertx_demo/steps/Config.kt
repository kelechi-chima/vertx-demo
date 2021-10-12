package com.demo.vertx_demo.steps

import io.vertx.amqp.AmqpClientOptions
import io.vertx.ext.mail.MailConfig
import io.vertx.pgclient.PgConnectOptions

fun pgConnectOptions() = PgConnectOptions().apply {
    host = "localhost"
    database = "steps"
    user = "postgres"
    password = "vertxinaction"
}

fun consumerConfig(groupId: String) = mapOf(
    "bootstrap.servers" to "localhost:19092",
    "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" to "io.vertx.kafka.client.serialization.JsonObjectDeserializer",
    "auto.offset.reset" to "earliest",
    "enable.auto.commit" to "true",
    "group.id" to groupId
)

fun producerConfig() = mapOf(
    "bootstrap.servers" to "localhost:19092",
    "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" to "io.vertx.kafka.client.serialization.JsonObjectSerializer",
    "acks" to "1"
)

fun amqpConfig() = AmqpClientOptions().apply {
    host = "localhost"
    port = 5672
//    username = "artemis"
//    password = "vertxinaction"
}

fun mailConfig() = MailConfig().apply {
    hostname = "localhost"
    port = 1025
}