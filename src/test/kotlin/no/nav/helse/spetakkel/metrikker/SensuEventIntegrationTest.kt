package no.nav.helse.spetakkel.metrikker

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import kotlin.streams.toList

class SensuEventIntegrationTest {

    companion object {
        private val server = ServerSocket(0)

        private val objectMapper = jacksonObjectMapper()
                .registerModule(JavaTimeModule())

        @AfterAll
        @JvmStatic
        fun `close server socket`() {
            server.close()
        }
    }

    private fun readFromSocket() =
            with (server) {
                soTimeout = 1000

                accept().use { socket ->
                    val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                    reader.lines().toList()
                }
            }

    @Test
    fun `should send event without fields and tags`() {
        val client = SensuMetricReporter("localhost", server.localPort, "check-app")

        val dataPoint = InfluxDBDataPoint("myEvent", mapOf("field1" to "val1", "field2" to 1), mapOf("tag1" to "tag2"))
        client.report(dataPoint)

        val jsonString = readFromSocket().joinToString(separator = "")
        val json = objectMapper.readTree(jsonString)

        assertEquals("check-app", json.path("name").asText())
        assertEquals("metric", json.path("type").asText())
        assertTrue("events_nano" in json.path("handlers").map(JsonNode::asText))
        assertEquals(dataPoint.toLineProtocol(), json.path("output").asText())
        assertEquals(0, json.path("status").asInt())

    }
}
