package com.demo.vertx_demo.jukebox

import io.vertx.core.AbstractVerticle
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.net.NetSocket
import io.vertx.core.parsetools.RecordParser
import org.slf4j.LoggerFactory

class NetControl : AbstractVerticle() {

    private val logger = LoggerFactory.getLogger(NetControl::class.java)

    override fun start() {
        vertx.createNetServer()
            .connectHandler(this::handleClient)
            .listen(3000)
    }

    private fun handleClient(socket: NetSocket) {
        RecordParser.newDelimited("\n", socket)
            .handler { buffer -> handleBuffer(socket, buffer) }
            .endHandler { logger.info("connection ended") }
    }

    private fun handleBuffer(socket: NetSocket, buffer: Buffer) {
        when (val command = buffer.toString()) {
            "/list" -> listCommand(socket)
            "/play" -> vertx.eventBus().send(JUKEBOX_PLAY, "")
            "/pause" -> vertx.eventBus().send(JUKEBOX_PAUSE, "")
            else -> {
                if (command.startsWith("/schedule")) {
                    schedule(command)
                } else {
                    socket.write("Unknown command\n")
                }
            }
        }
    }

    private fun listCommand(socket: NetSocket) {
        vertx.eventBus().request<JsonObject>(JUKEBOX_LIST, "") { reply ->
            if (reply.succeeded()) {
                val data = reply.result().body()
                data.getJsonArray("files").forEach { file -> socket.write("$file\n") }
            } else {
                logger.error("/list error", reply.cause())
            }
        }
    }

    private fun schedule(command: String) {
        val track = command.substring(10)
        vertx.eventBus().send(JUKEBOX_SCHEDULE, JsonObject().put("file", track))
    }
}