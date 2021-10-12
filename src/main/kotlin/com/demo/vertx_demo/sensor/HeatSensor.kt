package com.demo.vertx_demo.sensor

import io.vertx.core.AbstractVerticle
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.JsonObject
import java.util.Random
import java.util.UUID

class HeatSensor(private val random: Random = Random(),
                 private val id: String = UUID.randomUUID().toString()) : AbstractVerticle() {
    private var temperature = 21.0

    override fun start() {
        vertx.createHttpServer()
            .requestHandler(this::handleRequest)
            .listen(config().getInteger("http.port", 3000))

        scheduleNextUpdate()
    }

    private fun handleRequest(request: HttpServerRequest) {
        val json = JsonObject().put("id", id).put("temp", temperature)
        request.response()
            .putHeader("Content-Type", "application/json")
            .end(json.encode())
    }

    private fun scheduleNextUpdate() {
        vertx.setTimer(random.nextInt(5000) + 1000L, this::update)
    }

    private fun update(timerId: Long) {
        temperature += (delta() / 10)
        val payload = JsonObject().put("id", id).put("temp", temperature)
        vertx.eventBus().publish(SENSOR_UPDATES, payload)
        scheduleNextUpdate()
    }

    private fun delta(): Double {
        return if (random.nextInt() > 0) {
            random.nextGaussian()
        } else {
            -random.nextGaussian()
        }
    }
}