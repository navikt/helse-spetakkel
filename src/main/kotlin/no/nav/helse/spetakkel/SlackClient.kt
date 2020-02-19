package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URL

internal class SlackClient(
    private val webhookUrl: String,
    private val defaultChannel: String? = null,
    private val defaultUsername: String? = null
) {

    private companion object {
        private val log = LoggerFactory.getLogger(SlackClient::class.java)
        private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    fun postMessage(text: String, icon: String? = null) {
        webhookUrl.post(objectMapper.writeValueAsString(mutableMapOf("text" to text).apply {
            icon?.let { this.put("icon_emoji", it) }
            defaultChannel?.let { this.put("channel", it) }
            defaultUsername?.let { this.put("username", it) }
        }))
    }

    private fun String.post(jsonPayload: String) {
        try {
            val connection = (URL(this).openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                connectTimeout = 1000
                readTimeout = 1000
                doOutput = true
                setRequestProperty("Content-Type", "application/json; charset=utf-8")
                setRequestProperty("User-Agent", "navikt/spetakkel")

                outputStream.use { it.bufferedWriter(Charsets.UTF_8).apply { write(jsonPayload); flush() } }
            }

            val responseCode = connection.responseCode

            if (connection.responseCode !in 200..299) {
                return log.error("response from slack: code=$responseCode body=${connection.errorStream.readText()}")
            }

            log.info("response from slack: code=$responseCode body=${connection.inputStream.readText()}")
        } catch (err: IOException) {
            log.error("feil ved posting til slack: {}", err)
        }
    }

    private fun InputStream.readText() = use { it.bufferedReader().readText() }
}
