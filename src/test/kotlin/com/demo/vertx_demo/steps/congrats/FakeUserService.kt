package com.demo.vertx_demo.steps.congrats

import com.demo.vertx_demo.steps.APPLICATION_JSON
import com.demo.vertx_demo.steps.DEVICE_ID
import com.demo.vertx_demo.steps.EMAIL
import com.demo.vertx_demo.steps.USERNAME
import io.reactivex.Completable
import io.vertx.core.json.JsonObject
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.ext.web.Router
import io.vertx.reactivex.ext.web.RoutingContext
import org.slf4j.LoggerFactory

class FakeUserService : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(FakeUserService::class.java)

    override fun rxStart(): Completable {
        val router = Router.router(vertx)
        router.get("/owns/:$DEVICE_ID").handler(this::owns)
        router.get("/:$USERNAME").handler(this::username)
        return vertx.createHttpServer()
            .requestHandler(router)
            .rxListen(3000)
            .ignoreElement()
    }

    private fun username(ctx: RoutingContext) {
        logger.info("User data request ${ctx.request().path()}")
        val payload = JsonObject().put(USERNAME, "Foo").put(EMAIL, "foo@mail.tld")
        ctx.response()
            .putHeader("Content-Type", APPLICATION_JSON)
            .end(payload.encode())
    }

    private fun owns(ctx: RoutingContext) {
        logger.info("Device ownership request ${ctx.request().path()}")
        val payload = JsonObject().put(USERNAME, "Foo")
        ctx.response()
            .putHeader("Content-Type", APPLICATION_JSON)
            .end(payload.encode())
    }
}