package com.demo.vertx_demo.sensor

import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.logging.SLF4JLogDelegateFactory
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import org.slf4j.LoggerFactory

fun main() {
    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory::class.java.canonicalName)
    System.setProperty("hazelcast.logging.type", "slf4j")
    val logger = LoggerFactory.getLogger("FirstInstance")

    val options = VertxOptions().setClusterManager(HazelcastClusterManager())
    Vertx.clusteredVertx(options) { ar ->
        if (ar.succeeded()) {
            logger.info("First instance has been started")
            val vertx = ar.result()
            vertx.deployVerticle(HeatSensor::class.java.canonicalName, DeploymentOptions().setInstances(4))
            vertx.deployVerticle(HttpServer::class.java.canonicalName)
        } else {
            logger.error("Could not start", ar.cause())
        }
    }
}