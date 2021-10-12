package com.demo.vertx_demo.sensor

import io.vertx.core.AbstractVerticle
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.predicate.ResponsePredicate
import io.vertx.ext.web.codec.BodyCodec
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger

class CollectorService : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(CollectorService::class.java)
    private lateinit var client: WebClient

    override fun start() {
        client = WebClient.create(vertx)
        vertx.createHttpServer()
            .requestHandler(this::handleRequest)
            .listen(8080)
    }

    private fun handleRequest(request: HttpServerRequest) {
        val responses = mutableListOf<JsonObject>()
        val counter = AtomicInteger()
        (0 until 3).forEach { i ->
            client.get(3000 + i, "localhost", "/")
                .expect(ResponsePredicate.SC_SUCCESS)
                .`as`(BodyCodec.jsonObject())
                .send { ar ->
                    if (ar.succeeded()) {
                        responses.add(ar.result().body())
                    } else {
                        logger.error("Sensor at port ${3000 + i} down?", ar.cause())
                    }
                    if (counter.incrementAndGet() == 3) {
                        val data = JsonObject().put("data", JsonArray(responses))
                        sendToSnapshot(request, data)
                    }
                }
        }
    }

    private fun sendToSnapshot(request: HttpServerRequest, data: JsonObject) {
        client.post(4000, "localhost", "/")
            .expect(ResponsePredicate.SC_SUCCESS)
            .sendJsonObject(data) { ar ->
                if (ar.succeeded()) {
                    sendResponse(request, data)
                } else {
                    logger.error("Snapshot down?", ar.cause())
                    request.response().setStatusCode(500).end()
                }
            }
    }

    private fun sendResponse(request: HttpServerRequest, data: JsonObject) {
        request.response().putHeader("Content-Type", "application/json")
            .end(data.encode())
    }
}