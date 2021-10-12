package com.demo.vertx_demo.sensor

import io.vertx.core.AbstractVerticle
import io.vertx.core.CompositeFuture
import io.vertx.core.Future
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.predicate.ResponsePredicate
import io.vertx.ext.web.codec.BodyCodec
import org.slf4j.LoggerFactory

class CollectorServiceWithFutures : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(CollectorServiceWithFutures::class.java)
    private lateinit var client: WebClient

    override fun start() {
        client = WebClient.create(vertx)
        vertx.createHttpServer()
            .requestHandler(this::handleRequest)
            .listen(8080)
    }

    private fun handleRequest(request: HttpServerRequest) {
        CompositeFuture.all(
            fetchTemperature(3000),
            fetchTemperature(3001),
            fetchTemperature(3002))
            .flatMap(this::sendToSnapshot)
            .onSuccess { data ->
                request.response().putHeader("Content-Type", "application/json").end(data.encode())
            }
            .onFailure { err ->
                logger.error("Something went wrong", err)
                request.response().setStatusCode(500).end()
            }
    }

    private fun fetchTemperature(port: Int): Future<JsonObject> {
        return client
            .get(port, "localhost", "/")
            .expect(ResponsePredicate.SC_SUCCESS)
            .`as`(BodyCodec.jsonObject())
            .send()
            .map(HttpResponse<JsonObject>::body)
    }

    private fun sendToSnapshot(temps: CompositeFuture): Future<JsonObject> {
        val tempData = temps.list<JsonObject>()
        val data = JsonObject().put("data", JsonArray().add(tempData[0]).add(tempData[1]).add(tempData[2]))
        return client
            .post(4000, "localhost", "/")
            .expect(ResponsePredicate.SC_SUCCESS)
            .sendJson(data)
            .map { data }
    }

    private fun sendResponse(request: HttpServerRequest, data: JsonObject) {
        request.response()
            .putHeader("Content-Type", "application/json")
            .end(data.encode())
    }
}