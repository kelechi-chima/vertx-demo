package com.demo.vertx_demo.sensor

import io.vertx.core.AbstractVerticle
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject

class SensorData(private val lastValues: HashMap<String, Double> = hashMapOf()) : AbstractVerticle() {

    override fun start() {
        vertx.eventBus().consumer(SENSOR_UPDATES, this::update)
        vertx.eventBus().consumer(SENSOR_AVERAGE, this::average)
    }

    private fun update(msg: Message<JsonObject>) {
        val body = msg.body()
        val id = body.getString("id")
        val temp = body.getDouble("temp")
        lastValues[id] = temp
    }

    private fun average(msg: Message<JsonObject>) {
        val average = lastValues.values.average()
        msg.reply(JsonObject().put("average", average))
    }
}