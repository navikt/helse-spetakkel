package no.nav.helse.spetakkel

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

internal class PåminnelseMonitorTest {

    companion object {

        private val wireMockServer = WireMockServer(WireMockConfiguration.options().dynamicPort())
        private lateinit var webhookUrl: String
        private const val webhookPath = "/webhook"

        @BeforeAll
        @JvmStatic
        fun setup() {
            wireMockServer.start()
            webhookUrl = wireMockServer.baseUrl() + webhookPath
        }

        @AfterAll
        @JvmStatic
        fun teardown(){
            wireMockServer.stop()
        }
    }

    private val client = create().port(wireMockServer.port()).build()
    private val rapid = TestRapid()
    private lateinit var monitor: PåminnelseMonitor

    @BeforeEach
    fun init() {
        client.resetRequests()

        post(webhookPath).willReturn(ok())

        monitor = PåminnelseMonitor(rapid, webhookUrl)
    }

    @Test
    fun `lager ikke alert ved påminnelse nr 1`() {
        rapid.sendTestMessage(påminnelse(1))
        client.verifyThat(0, postRequestedFor(urlEqualTo(webhookPath)))
    }

    @Test
    fun `lager alert ved påminnelse nr 2`() {
        rapid.sendTestMessage(påminnelse(2))
        client.verifyThat(1, postRequestedFor(urlEqualTo(webhookPath)))
    }

    private fun påminnelse(antallGangerPåminnet: Int) = """
{
    "@event_name": "påminnelse",
    "vedtaksperiodeId": "${UUID.randomUUID()}",
    "antallGangerPåminnet": $antallGangerPåminnet,
    "endringstidspunkt": "${LocalDateTime.now()}",
    "tilstand": "SOME_STATE"
}
"""

    private class TestRapid : RapidsConnection() {
        private val context = TestContext()

        fun sendTestMessage(message: String) {
            listeners.forEach { it.onMessage(message, context) }
        }

        override fun publish(message: String) {
            error("not implemented")
        }

        override fun publish(key: String, message: String) {
            error("not implemented")
        }

        override fun start() {
            error("not implemented")
        }

        override fun stop() {
            error("not implemented")
        }

        private class TestContext : MessageContext {
            override fun send(message: String) {
                error("not implemented")
            }

            override fun send(key: String, message: String) {
                error("not implemented")
            }
        }
    }
}
