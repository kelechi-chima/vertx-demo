package com.demo.vertx_demo

fun main() {
//    System.setProperty("vertx.logger-delegate-factory-class-name", SLF4JLogDelegateFactory::class.java.canonicalName)
//    val logger = LoggerFactory.getLogger("App")
//    val vertx = Vertx.vertx()
//
//    vertx.rxDeployVerticle(UserProfileApiVerticle())
//        .subscribe(
//            { logger.info("UserProfile HTTP server started on port 3000") },
//            { err -> logger.error("Woops", err) }
//        )
//
//    vertx.rxDeployVerticle(ActivityApiVerticle())
//        .subscribe(
//            { logger.info("Activity API HTTP server started on port 3001") },
//            { err -> logger.error("Woops", err) }
//        )
//
//    vertx.rxDeployVerticle(IngesterVerticle())
//        .subscribe(
//            { logger.info("Ingester HTTP server started on port 3002") },
//            { err -> logger.error("Woops", err) }
//        )
//
//    vertx.rxDeployVerticle(EventsVerticle())
//        .subscribe(
//            { logger.info("Events verticle started") },
//            { err -> logger.error("Woops", err) }
//        )
//
//    vertx.rxDeployVerticle(CongratsVerticle())
//        .subscribe(
//            { logger.info("Congrats verticle started") },
//            { err -> logger.error("Woops", err) }
//        )
//
//    vertx.rxDeployVerticle(EventStatsVerticle())
//        .subscribe(
//            { logger.info("Event stats verticle started") },
//            { err -> logger.error("Woops", err) }
//        )

    println(rotationalCipher("Zebra-493", 3))
}

fun rotationalCipher(input: String, rotationFactor: Int): String {
//    input.chars().map { i ->
//        i + rotationFactor
//    }
    println('A'.code)
    val output = StringBuilder()
    for (c in input) {
        if (c.isLetter()) {
            //output.append(c + rotationFactor)
            output.append((c.code + rotationFactor).toChar())
        } else if (c.isDigit()) {
            val newDigit = c.digitToInt() + rotationFactor
            output.append(if (newDigit < 10) newDigit else newDigit - 10 )
        } else {
            output.append(c)
        }
    }
    return output.toString()
}