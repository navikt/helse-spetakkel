package no.nav.helse.spetakkel

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.server.engine.ApplicationEngine
import io.ktor.server.engine.applicationEngineEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import no.nav.helse.spetakkel.nais.nais
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

fun main() {
    launchApp(System.getenv())
}

private val log = LoggerFactory.getLogger("no.nav.helse.spetakkel.App")

fun isApplicationAlive() = true
fun isApplicationReady() = true

private val closed = AtomicBoolean(false)

private val objectMapper = jacksonObjectMapper()
        .registerModule(JavaTimeModule())

fun launchApp(env: Map<String, String>): ApplicationEngine {
    Thread.currentThread().setUncaughtExceptionHandler { thread, err ->
        log.error("Uncaught exception in thread ${thread.name}: ${err.message}", err)
    }

    val app = launchKtor(env)

    val kafkaConfig = KafkaConfigBuilder(
            bootstrapServers = env.getValue("KAFKA_BOOTSTRAP_SERVERS"),
            username = "/var/run/secrets/nais.io/service_user/username".readFile(),
            password = "/var/run/secrets/nais.io/service_user/password".readFile(),
            truststore = env.getValue("NAV_TRUSTSTORE_PATH"),
            truststorePassword = env.getValue("NAV_TRUSTSTORE_PASSWORD")
    )

    val consumer = KafkaConsumer(kafkaConfig.consumerConfig(), StringDeserializer(), StringDeserializer())

    Runtime.getRuntime().addShutdownHook(Thread {
        closed.set(true)
        consumer.wakeup()
        app.stop(1, 1, TimeUnit.SECONDS)
    })

    try {
        consumer.subscribe(listOf("privat-helse-sykepenger-rapid-v1"))
        while (!closed.get()) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach {
                log.info("leste offset ${it.offset()}")
            }
        }
    } catch (err: WakeupException) {
        // Ignore exception if closing
        if (!closed.get()) throw err
    } finally {
        consumer.close()
    }

    return app
}

private fun launchKtor(env: Map<String, String>): ApplicationEngine {
    val app = embeddedServer(Netty, applicationEngineEnvironment {
        log = no.nav.helse.spetakkel.log

        connector {
            port = env["HTTP_PORT"]?.toInt() ?: 8080
        }

        module {
            nais(::isApplicationAlive, ::isApplicationReady)
        }
    })

    app.start(wait = false)

    return app
}

private fun String.readFile() =
        try {
            File(this).readText(Charsets.UTF_8)
        } catch (err: FileNotFoundException) {
            null
        }
