package com.demo.vertx_demo.sensor

import io.vertx.core.AbstractVerticle
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.JsonObject

class HttpServer : AbstractVerticle() {

    override fun start() {
        vertx.createHttpServer()
            .requestHandler(this::handler)
            .listen(config().getInteger("port", 8080))
    }

    private fun handler(request: HttpServerRequest) {
        when {
            "/" == request.path() -> {
                request.response().sendFile("index.html")
            }
            "/sse" == request.path() -> {
                sse(request)
            }
            else -> {
                request.response().statusCode = 404
            }
        }
    }

    private fun sse(request: HttpServerRequest) {
        val response = request.response()
        response.putHeader("Content-Type", "text/event-stream")
            .putHeader("Cache-Control", "no-cache")
            .isChunked = true

        val consumer = vertx.eventBus().consumer<JsonObject>(SENSOR_UPDATES)
        consumer.handler { msg ->
            response.write("event: update\n")
            response.write("data: ${msg.body().encode()}\n\n")
        }

        val ticks = vertx.periodicStream(1000)
        ticks.handler {
            vertx.eventBus().request<JsonObject>(SENSOR_AVERAGE, "") { reply ->
                if (reply.succeeded()) {
                    response.write("event: average\n")
                    response.write("data: ${reply.result().body().encode()}\n\n")
                }
            }
        }

        response.endHandler {
            consumer.unregister()
            ticks.cancel()
        }
    }
}