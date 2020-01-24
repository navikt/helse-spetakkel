package no.nav.helse.spetakkel.metrikker

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.OutputStreamWriter
import java.net.ConnectException
import java.net.Socket

class SensuEvent(
        private val name: String,
        private val type: String,
        private val handler: String,
        private val output: String,
        private val status: Int = 0
) {
    fun send(hostname: String, port: Int) {
        val json = asJson()
        writeToSocket(hostname, port, json)
        log.debug("Sent event({}) via probe-client", json)
    }

    private fun asJson() = objectMapper.writeValueAsString(mapOf(
        "name" to name,
        "type" to type,
        "handlers" to listOf(handler),
        "output" to output,
        "status" to status
    ))

    private companion object {
        private val objectMapper = jacksonObjectMapper()
                .registerModule(JavaTimeModule())

        private val log = LoggerFactory.getLogger(SensuEvent::class.java)

        fun writeToSocket(hostname: String, port: Int, data: String) {
            try {
                Socket(hostname, port).use { socket ->
                    try {
                        OutputStreamWriter(socket.getOutputStream(), "UTF-8").use { osw ->
                            osw.write(data, 0, data.length)
                            osw.flush()
                            log.debug("wrote {} bytes of data", data.length)
                        }
                    } catch (err: IOException) {
                        log.error("Unable to write data {} to socket", data, err)
                    }
                }
            } catch (err: ConnectException) {
                log.error("Unable to connect to {}:{} {}", hostname, port, err.message)
            } catch (err: IOException) {
                log.error("Unable to connect to {}:{} because of IO problems", hostname, port, err)
            } catch (err: Exception) {
                log.error("Unable to send event via probe-client", err)
            }
        }
    }
}
