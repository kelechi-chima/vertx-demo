package com.demo.vertx_demo.sensor

import io.vertx.core.AbstractVerticle
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.text.DecimalFormat

class LoggingListener : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(LoggingListener::class.java)
    private val format = DecimalFormat("#.##")

    override fun start() {
        vertx.eventBus().consumer(SENSOR_UPDATES, this::log)
    }

    private fun log(msg: Message<JsonObject>) {
        val body = msg.body()
        logger.info("${body.getString("id")}: ${format.format(body.getDouble("temp"))}")
    }
}