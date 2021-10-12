package com.demo.vertx_demo.jukebox

import io.vertx.core.AbstractVerticle
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.file.AsyncFile
import io.vertx.core.file.OpenOptions
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.http.HttpServerResponse
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.io.File
import java.net.URLDecoder
import java.util.ArrayDeque
import java.util.Queue

const val JUKEBOX_LIST = "jukebox.list"
const val JUKEBOX_SCHEDULE = "jukebox.schedule"
const val JUKEBOX_PLAY = "jukebox.play"
const val JUKEBOX_PAUSE = "jukebox.pause"

enum class State { PLAYING, PAUSED }

/**
 * Verticle that handles commands that are sent over TCP protocol, eg. netcat.
 */
class Jukebox(private var currentMode: State = State.PAUSED,
              private val streamers: MutableSet<HttpServerResponse> = HashSet(),
              private val playlist: Queue<String> = ArrayDeque()) : AbstractVerticle() {

    private val logger = LoggerFactory.getLogger(Jukebox::class.java)

    private var currentFile: AsyncFile? = null
    private var positionInFile = 0L

    override fun start() {
        val eventBus = vertx.eventBus()
        eventBus.consumer<Message<*>>(JUKEBOX_LIST, this::list)
        eventBus.consumer(JUKEBOX_SCHEDULE, this::schedule)
        eventBus.consumer<Message<*>>(JUKEBOX_PLAY, this::play)
        eventBus.consumer<Message<*>>(JUKEBOX_PAUSE, this::pause)

        vertx.createHttpServer()
            .requestHandler(this::httpHandler)
            .listen(8080)

        vertx.setPeriodic(100, this::streamAudioChunk)
    }

    private fun play(request: Message<*>) {
        currentMode = State.PLAYING
    }

    private fun pause(request: Message<*>) {
        currentMode = State.PAUSED
    }

    private fun schedule(request: Message<JsonObject>) {
        val file = request.body().getString("file")
        if (playlist.isEmpty() && currentMode == State.PAUSED) {
            currentMode = State.PLAYING
        }
        playlist.offer(file)
    }

    private fun list(request: Message<*>) {
        val userDir = System.getProperty("user.dir")
        logger.info(userDir)
        vertx.fileSystem().readDir("tracks", ".*mp3$") { ar ->
            if (ar.succeeded()) {
                val files = ar.result()
                    .map { name -> File(name) }
                    .map { file -> file.name }
                val json = JsonObject().put("files", JsonArray(files))
                request.reply(json)
            } else {
                logger.error("readDir failed", ar.cause())
                request.fail(500, ar.cause().message)
            }
        }
    }

    private fun httpHandler(request: HttpServerRequest) {
        if ("/" == request.path()) {
            openAudioStream(request)
            return
        }
        if (request.path().startsWith("/download/")) {
            val path = URLDecoder.decode(request.path(), "UTF-8")
            val sanitizedPath = path.substring(10).replace("/", "")
            download(sanitizedPath, request)
            return
        }
        request.response().setStatusCode(404).end()
    }

    private fun openAudioStream(request: HttpServerRequest) {
        val response = request.response().putHeader("Content-Type", "audio/mpeg").setChunked(true)
        streamers.add(response)
        response.endHandler {
            streamers.remove(response)
            logger.info("A streamer left")
        }
    }

    private fun  download(path: String, request: HttpServerRequest) {
        val file = "tracks/$path"
        if (!vertx.fileSystem().existsBlocking(file)) {
            request.response().setStatusCode(404).end()
            return
        }
        val options = OpenOptions().setRead(true)
        vertx.fileSystem().open(file, options) { ar ->
            if (ar.succeeded()) {
                downloadFile(ar.result(), request)
            } else {
                logger.error("Read failed", ar.cause())
                request.response().setStatusCode(500).end()
            }
        }
    }

    private fun downloadFile(file: AsyncFile, request: HttpServerRequest) {
        val response = request.response()
        response.setStatusCode(200).putHeader("Content-Type", "audio/mpeg").isChunked = true
        file.pipeTo(response)
    }

    private fun streamAudioChunk(id: Long) {
        if (currentMode == State.PAUSED) return
        if (currentFile == null && playlist.isEmpty()) {
            currentMode = State.PAUSED
            return
        }
        if (currentFile == null) {
            openNextFile()
        }
        currentFile?.read(Buffer.buffer(4096), 0, positionInFile, 4096) { ar ->
            if (ar.succeeded()) {
                processReadBuffer(ar.result())
            } else {
                logger.error("Read failed", ar.cause())
                closeCurrentFile()
            }
        }
    }

    private fun openNextFile() {
        val openOptions = OpenOptions().setRead(true)
        currentFile = vertx.fileSystem().openBlocking("tracks/${playlist.poll()}", openOptions)
        positionInFile = 0L
    }

    private fun processReadBuffer(buffer: Buffer) {
        positionInFile += buffer.length()
        if (buffer.length() == 0) {
            closeCurrentFile()
            return
        }
        streamers.forEach { streamer ->
            if (!streamer.writeQueueFull()) {
                streamer.write(buffer.copy())
            }
        }
    }

    private fun closeCurrentFile() {
        positionInFile = 0
        currentFile?.close()
        currentFile = null
    }
}