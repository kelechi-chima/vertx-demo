package com.demo.vertx_demo.sensor

import io.vertx.core.AbstractVerticle
import io.vertx.core.http.HttpMethod
import org.slf4j.LoggerFactory

class SnapshotService : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(SnapshotService::class.java)

    override fun start() {
        vertx.createHttpServer()
            .requestHandler { request ->
                if (request.method() != HttpMethod.POST || "application/json" != request.getHeader("Content-Type")) {
                    request.response().setStatusCode(400).end()
                }
                request.bodyHandler { buffer ->
                    logger.info("Latest temperatures: ${buffer.toJsonObject().encodePrettily()}")
                    request.response().end()
                }
            }
            .listen(config().getInteger("http.port", 4000))
    }
}