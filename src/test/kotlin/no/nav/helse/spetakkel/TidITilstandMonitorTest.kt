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
import kotlin.time.ExperimentalTime

@ExperimentalTime
internal class TidITilstandMonitorTest {

    companion object {

        private val wireMockServer = WireMockServer(WireMockConfiguration.options().dynamicPort())
        private lateinit var client: WireMock

        private lateinit var webhookUrl: String
        private const val webhookPath = "/webhook"

        @BeforeAll
        @JvmStatic
        fun setup() {
            wireMockServer.start()
            webhookUrl = wireMockServer.baseUrl() + webhookPath

            client = create().port(wireMockServer.port()).build().apply {
                configureFor(this)
            }

            stubFor(post(webhookPath).willReturn(ok()))
        }

        @AfterAll
        @JvmStatic
        fun teardown(){
            wireMockServer.stop()
        }
    }

    private val rapid = TestRapid()
    private lateinit var monitor: TidITilstandMonitor

    @BeforeEach
    fun init() {
        client.resetRequests()
        monitor = TidITilstandMonitor(rapid, SlackClient(webhookUrl, "#test-channel", "a_bot_name"))
    }

    @Test
    fun `lager ikke alert ved tid_i_tilstand mindre enn timeout`() {
        rapid.sendTestMessage(tidITilstand(3600, 3599))
        verify(0, postRequestedFor(urlEqualTo(webhookPath)))
    }

    @Test
    fun `lager alert ved tid_i_tilstand større enn timeout`() {
        rapid.sendTestMessage(tidITilstand(3600, 4200))
        verify(1, postRequestedFor(urlEqualTo(webhookPath))
            .withRequestBody(matchingJsonPath("$.channel", equalTo("#test-channel")))
            .withRequestBody(matchingJsonPath("$.username", equalTo("a_bot_name")))
            .withRequestBody(matchingJsonPath("$.icon_emoji", equalTo(":face_with_raised_eyebrow:")))
            .withRequestBody(matchingJsonPath("$.text", matching("Vedtaksperiode .* kom seg videre fra A_STATE_NAME til A_NEW_STATE_NAME etter 1 time og 10 minutter siden .* Forventet tid i tilstand var 1 time")))
        )
    }

    private fun tidITilstand(timeout: Int, tidITilstand: Int) = """
{
    "@event_name":"vedtaksperiode_tid_i_tilstand",
    "aktørId":"${UUID.randomUUID()}",
    "fødselsnummer":"${UUID.randomUUID()}",
    "organisasjonsnummer":"${UUID.randomUUID()}",
    "vedtaksperiodeId":"${UUID.randomUUID()}",
    "tilstand":"A_STATE_NAME",
    "nyTilstand":"A_NEW_STATE_NAME",
    "starttid":"${LocalDateTime.now()}",
    "sluttid":"${LocalDateTime.now()}",
    "timeout":$timeout,
    "tid_i_tilstand": $tidITilstand
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
