package com.demo.vertx_demo.steps.activities

import com.demo.vertx_demo.steps.APPLICATION_JSON
import com.demo.vertx_demo.steps.DAILY_STEPS_QUERY
import com.demo.vertx_demo.steps.DEVICE_ID
import com.demo.vertx_demo.steps.LAST_24_HOURS_RANKING_QUERY
import com.demo.vertx_demo.steps.MONTHLY_STEPS_QUERY
import com.demo.vertx_demo.steps.STEPS_COUNT
import com.demo.vertx_demo.steps.TOTAL_STEPS_COUNT_QUERY
import com.demo.vertx_demo.steps.pgConnectOptions
import io.reactivex.Completable
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.ext.web.Router
import io.vertx.reactivex.ext.web.RoutingContext
import io.vertx.reactivex.pgclient.PgPool
import io.vertx.reactivex.sqlclient.Row
import io.vertx.reactivex.sqlclient.RowSet
import io.vertx.reactivex.sqlclient.Tuple
import io.vertx.sqlclient.PoolOptions
import org.slf4j.LoggerFactory
import java.time.DateTimeException
import java.time.LocalDateTime

class ActivityApiVerticle : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(ActivityApiVerticle::class.java)

    private lateinit var pgPool: PgPool

    override fun rxStart(): Completable {
        pgPool = PgPool.pool(vertx, pgConnectOptions(), PoolOptions())

        val router = Router.router(vertx)
        router.get("/:$DEVICE_ID/total").handler(this::totalSteps)
        router.get("/:$DEVICE_ID/:year/:month").handler(this::stepsOnMonth)
        router.get("/:$DEVICE_ID/:year/:month/:day").handler(this::stepsOnDay)
        router.get("/ranking-last-24-hours").handler(this::ranking)

        return vertx.createHttpServer()
            .requestHandler(router)
            .rxListen(config().getInteger("http.port", 3001))
            .ignoreElement()
    }

    private fun totalSteps(ctx: RoutingContext) {
        val deviceId = ctx.pathParam(DEVICE_ID)
        val params = Tuple.of(deviceId)
        pgPool
            .preparedQuery(TOTAL_STEPS_COUNT_QUERY)
            .rxExecute(params)
            .map { rs -> rs.iterator().next() }
            .subscribe(
                { row -> sendCount(ctx, row) },
                { err -> handleError(ctx, err) }
            )
    }

    private fun stepsOnMonth(ctx: RoutingContext) {
        try {
            val deviceId = ctx.pathParam(DEVICE_ID)
            val dateTime = LocalDateTime.of(
                ctx.pathParam("year").toInt(),
                ctx.pathParam("month").toInt(),
                1, 0, 0
            )
            val params = Tuple.of(deviceId, dateTime)
            pgPool
                .preparedQuery(MONTHLY_STEPS_QUERY)
                .rxExecute(params)
                .map { rs -> rs.iterator().next() }
                .subscribe(
                    { row -> sendCount(ctx, row) },
                    { err -> handleError(ctx, err) }
                )
        } catch (e: DateTimeException) {
            sendBadRequest(ctx)
        } catch (e: NumberFormatException) {
            sendBadRequest(ctx)
        }
    }

    private fun stepsOnDay(ctx: RoutingContext) {
        try {
            val deviceId = ctx.pathParam(DEVICE_ID)
            val dateTime = LocalDateTime.of(
                ctx.pathParam("year").toInt(),
                ctx.pathParam("month").toInt(),
                ctx.pathParam("day").toInt(),
                0, 0
            )
            val params = Tuple.of(deviceId, dateTime)
            pgPool
                .preparedQuery(DAILY_STEPS_QUERY)
                .rxExecute(params)
                .map { rs -> rs.iterator().next() }
                .subscribe(
                    { row -> sendCount(ctx, row) },
                    { err -> handleError(ctx, err) }
                )
        } catch (e: DateTimeException) {
            sendBadRequest(ctx)
        } catch (e: NumberFormatException) {
            sendBadRequest(ctx)
        }
    }

    private fun ranking(ctx: RoutingContext) {
        pgPool
            .preparedQuery(LAST_24_HOURS_RANKING_QUERY)
            .rxExecute()
            .subscribe(
                { rows -> sendRanking(ctx, rows) },
                { err -> handleError(ctx, err) }
            )
    }

    private fun sendRanking(ctx: RoutingContext, rows: RowSet<Row>) {
        val payload = JsonArray()
        rows.forEach { row ->
            payload.add(JsonObject()
                .put(DEVICE_ID, row.getValue(DEVICE_ID))
                .put(STEPS_COUNT, row.getValue("steps")))
        }
        ctx.response()
            .putHeader("Content-Type", APPLICATION_JSON)
            .end(payload.encode())
    }

    private fun sendCount(ctx: RoutingContext, row: Row) {
        val count = row.getInteger(0)
        return if (count == null) {
            ctx.response().setStatusCode(404).end()
        } else {
            val payload = JsonObject().put("count", count)
            ctx.response()
                .putHeader("Content-Type", APPLICATION_JSON)
                .end(payload.encode())
        }
    }

    private fun handleError(ctx: RoutingContext, err: Throwable) {
        logger.error("Woops", err)
        ctx.response().setStatusCode(500).end()
    }

    private fun sendBadRequest(ctx: RoutingContext) {
        ctx.response().setStatusCode(400).end()
    }

}