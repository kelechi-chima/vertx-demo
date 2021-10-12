package com.demo.vertx_demo.steps.userprofile

import com.demo.vertx_demo.steps.CITY
import com.demo.vertx_demo.steps.DEVICE_ID
import com.demo.vertx_demo.steps.EMAIL
import com.demo.vertx_demo.steps.ID
import com.demo.vertx_demo.steps.MAKE_PUBLIC
import com.demo.vertx_demo.steps.PASSWORD
import com.demo.vertx_demo.steps.USER
import com.demo.vertx_demo.steps.USERNAME
import io.reactivex.Completable
import io.reactivex.Maybe
import io.reactivex.MaybeSource
import io.reactivex.functions.Function
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.mongo.MongoAuthenticationOptions
import io.vertx.ext.auth.mongo.MongoAuthorizationOptions
import io.vertx.reactivex.core.AbstractVerticle
import io.vertx.reactivex.ext.auth.mongo.MongoAuthentication
import io.vertx.reactivex.ext.auth.mongo.MongoUserUtil
import io.vertx.reactivex.ext.mongo.MongoClient
import io.vertx.reactivex.ext.web.Router
import io.vertx.reactivex.ext.web.RoutingContext
import io.vertx.reactivex.ext.web.handler.BodyHandler
import org.slf4j.LoggerFactory
import java.util.regex.Pattern

class UserProfileApiVerticle : AbstractVerticle() {
    private val logger = LoggerFactory.getLogger(UserProfileApiVerticle::class.java)

    private val mongoConfig = JsonObject()
        .put("host", "localhost")
        .put("port", 27017)
        .put("db_name", "profiles")

    private val fetchFields = JsonObject()
        .put(ID, 0)
        .put(USERNAME, 1)
        .put(EMAIL, 1)
        .put(DEVICE_ID, 1)
        .put(CITY, 1)
        .put(MAKE_PUBLIC, 1)

    private val validUsername = Pattern.compile("\\w[\\w+|-]*")
    private val validDeviceId = Pattern.compile("\\w[\\w+|-]*")
    private val validEmail = Pattern.compile("^[a-zA-Z0-9_+&*-]+(?:\\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\\.)+[a-zA-Z]{2,7}\$")

    private lateinit var mongoClient: MongoClient
    private lateinit var authProvider: MongoAuthentication
    private lateinit var userUtil: MongoUserUtil

    override fun rxStart(): Completable? {
        mongoClient = MongoClient.createShared(vertx, mongoConfig)
        authProvider = MongoAuthentication.create(mongoClient, MongoAuthenticationOptions())
        userUtil = MongoUserUtil.create(mongoClient, MongoAuthenticationOptions(), MongoAuthorizationOptions())

        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())
        router.post("/register").handler(this::validateRegistration).handler(this::register)
        router.get("/:$USERNAME").handler(this::fetchUser)
        router.put("/:$USERNAME").handler(this::updateUser)
        router.post("/authenticate").handler(this::authenticate)
        router.get("/owns/:$DEVICE_ID").handler(this::whoOwns)

        return vertx.createHttpServer()
            .requestHandler(router)
            .rxListen(config().getInteger("http.port",3000))
            .ignoreElement()
    }

    // Registration ==============================================================================

    private fun validateRegistration(ctx: RoutingContext) {
        val body = jsonBody(ctx)
        if (anyRegistrationFieldIsMissing(body) || anyRegistrationFieldWrong(body)) {
            ctx.fail(400)
        } else {
            ctx.next()
        }
    }

    private fun anyRegistrationFieldIsMissing(body: JsonObject) =
        !(body.containsKey(USERNAME) &&
        body.containsKey(PASSWORD) &&
        body.containsKey(EMAIL) &&
        body.containsKey(CITY) &&
        body.containsKey(DEVICE_ID) &&
        body.containsKey(MAKE_PUBLIC))

    private fun anyRegistrationFieldWrong(body: JsonObject) =
        !validUsername.matcher(body.getString(USERNAME)).matches() ||
        !validEmail.matcher(body.getString(EMAIL)).matches() ||
        body.getString(PASSWORD).trim().isEmpty() ||
        !validDeviceId.matcher(body.getString(DEVICE_ID)).matches()

    private fun register(ctx: RoutingContext) {
        val body = jsonBody(ctx)
        val username = body.getString(USERNAME)
        val password = body.getString(PASSWORD)

        val extraInfo = JsonObject().put("\$set",
            JsonObject().put(EMAIL, body.getString(EMAIL))
                .put(CITY, body.getString(CITY))
                .put(DEVICE_ID, body.getString(DEVICE_ID))
                .put(MAKE_PUBLIC, body.getBoolean(MAKE_PUBLIC)))

        userUtil
            .rxCreateUser(username, password)
            .flatMapMaybe { docId -> insertExtraInfo(extraInfo, docId) }
            .ignoreElement()
            .subscribe(
                { completeRegistration(ctx) },
                { err: Throwable -> handleRegistrationError(ctx, err) })
    }

    private fun insertExtraInfo(extraInfo: JsonObject, docId: String): MaybeSource<JsonObject> {
        val query = JsonObject().put(ID, docId)
        return mongoClient
            .rxFindOneAndUpdate(USER, query, extraInfo)
            .onErrorResumeNext(Function { deleteIncompleteUser(query, it) })
    }

    private fun completeRegistration(ctx: RoutingContext) {
        ctx.response().end()
    }

    private fun handleRegistrationError(ctx: RoutingContext, err: Throwable) {
        if (isIndexViolated(err)) {
            logger.error("Registration failure: ${err.message}")
            ctx.fail(409)
        } else {
            fail500(ctx, err)
        }
    }

    private fun isIndexViolated(err: Throwable) = err.message!!.contains("E11000")

    private fun deleteIncompleteUser(query: JsonObject, err: Throwable): MaybeSource<JsonObject> {
        return if (isIndexViolated(err)) {
            mongoClient
                .rxRemoveDocument(USER, query)
                .flatMap { Maybe.error(err) }
        } else {
            Maybe.error(err)
        }
    }

    // Authentication ==============================================================================

    private fun authenticate(ctx: RoutingContext) {
        authProvider.rxAuthenticate(jsonBody(ctx))
            .subscribe(
                { completeEmptySuccess(ctx) },
                { err: Throwable -> handleAuthenticationError(ctx, err) }
            )
    }

    private fun handleAuthenticationError(ctx: RoutingContext, err: Throwable) {
        logger.error("Authentication failure: ${err.message}")
        ctx.response().setStatusCode(401).end()
    }

    // Fetch user ==============================================================================

    private fun fetchUser(ctx: RoutingContext) {
        val username = ctx.pathParam(USERNAME)
        val query = JsonObject().put(USERNAME, username)
        mongoClient
            .rxFindOne(USER, query, fetchFields)
            .toSingle()
            .subscribe(
                { json -> completeFetchRequest(ctx, json) },
                { err -> handleFetchError(ctx, err) }
            )
    }

    private fun completeFetchRequest(ctx: RoutingContext, json: JsonObject) {
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(json.encode())
    }

    private fun handleFetchError(ctx: RoutingContext, err: Throwable) {
        if (err is NoSuchElementException) {
            ctx.fail(404)
        } else {
            fail500(ctx, err)
        }
    }

    // Update user ==============================================================================

    private fun updateUser(ctx: RoutingContext) {
        val username = ctx.pathParam(USERNAME)
        val query = JsonObject().put(USERNAME, username)
        val updates = fieldsToUpdate(ctx)
        if (updates.isEmpty) {
            ctx.response().setStatusCode(200).end()
            return
        }
        val updateQuery = JsonObject().put("\$set", updates)
        mongoClient
            .rxFindOneAndUpdate(USER, query, updateQuery)
            .ignoreElement()
            .subscribe(
                { completeEmptySuccess(ctx) },
                { err -> handleUpdateError(ctx, err) }
            )
    }

    private fun fieldsToUpdate(ctx: RoutingContext): JsonObject {
        val body = jsonBody(ctx)
        val fields = JsonObject()
        if (body.containsKey(CITY)) fields.put(CITY, body.getString(CITY))
        if (body.containsKey(EMAIL)) fields.put(EMAIL, body.getString(EMAIL))
        if (body.containsKey(MAKE_PUBLIC)) fields.put(MAKE_PUBLIC, body.getBoolean(MAKE_PUBLIC))
        return fields
    }

    private fun handleUpdateError(ctx: RoutingContext, err: Throwable) {
        fail500(ctx, err)
    }

    // Device owner ==============================================================================

    private fun whoOwns(ctx: RoutingContext) {
        val deviceId = ctx.pathParam(DEVICE_ID)
        val query = JsonObject().put(DEVICE_ID, deviceId)
        val fields = JsonObject().put(ID, 0).put(USERNAME, 1).put(DEVICE_ID, 1)
        mongoClient
            .rxFindOne(USER, query, fields)
            .toSingle()
            .subscribe(
                { json -> completeFetchRequest(ctx, json) },
                { err -> handleFetchError(ctx, err) }
            )
    }

    private fun completeEmptySuccess(ctx: RoutingContext) {
        ctx.response().setStatusCode(200).end()
    }

    private fun fail500(ctx: RoutingContext, err: Throwable) {
        logger.error("Woops", err)
        ctx.fail(500)
    }

    private fun jsonBody(ctx: RoutingContext) =
        if (ctx.body.length() == 0) {
            JsonObject()
        } else {
            ctx.bodyAsJson
        }
}