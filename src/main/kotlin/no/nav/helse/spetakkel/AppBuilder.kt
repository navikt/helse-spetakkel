package no.nav.helse.spetakkel

import io.ktor.server.engine.ApplicationEngine
import io.ktor.server.engine.applicationEngineEnvironment
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import no.nav.helse.spetakkel.nais.nais
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileNotFoundException
import java.util.concurrent.TimeUnit

class AppBuilder(private val env: Map<String, String>) {
    private val log = LoggerFactory.getLogger(AppBuilder::class.java)

    private val kafkaConfig = KafkaConfigBuilder(
            bootstrapServers = env.getValue("KAFKA_BOOTSTRAP_SERVERS"),
            username = "/var/run/secrets/nais.io/service_user/username".readFile(),
            password = "/var/run/secrets/nais.io/service_user/password".readFile(),
            truststore = env.getValue("NAV_TRUSTSTORE_PATH"),
            truststorePassword = env.getValue("NAV_TRUSTSTORE_PASSWORD")
    )

    private val rapid = Rapid(kafkaConfig.consumerConfig(), "privat-helse-sykepenger-rapid-v1").apply {
        register(TilstandsendringsRiver())
    }

    private val app = createKtorApp(rapid::isRunning, rapid::isRunning)

    init {
        Thread.currentThread().setUncaughtExceptionHandler(::uncaughtExceptionHandler)
        Runtime.getRuntime().addShutdownHook(Thread(::stop))
    }

    fun start() {
        app.start(wait = false)
        rapid.start()
    }

    fun stop() {
        rapid.stop()
        app.stop(1, 1, TimeUnit.SECONDS)
    }

    private fun uncaughtExceptionHandler(thread: Thread, err: Throwable) {
        log.error("Uncaught exception in thread ${thread.name}: ${err.message}", err)
    }

    private fun createKtorApp(isAliveCheck: () -> Boolean = { true },
                              isReadyCheck: () -> Boolean = { true }): ApplicationEngine {
        return embeddedServer(Netty, applicationEngineEnvironment {
            log = this@AppBuilder.log

            connector {
                port = env["HTTP_PORT"]?.toInt() ?: 8080
            }

            module {
                nais(isAliveCheck, isReadyCheck)
            }
        })
    }
}

private fun String.readFile() =
        try {
            File(this).readText(Charsets.UTF_8)
        } catch (err: FileNotFoundException) {
            null
        }
