package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URL
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME

internal class PåminnelseMonitor(
    rapidsConnection: RapidsConnection,
    private val webhookUrl: String?
) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(PåminnelseMonitor::class.java)
        private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "påminnelse") }
            validate { it.requireKey("vedtaksperiodeId") }
            validate { it.requireKey("antallGangerPåminnet") }
            validate { it.requireKey("endringstidspunkt") }
            validate { it.requireKey("tilstand") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val påminnelse = Påminnelse(packet)
        if (2 > påminnelse.antallGangerPåminnet) return
        alert(påminnelse)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {}

    private fun alert(påminnelse: Påminnelse) {
        log.error(
            "vedtaksperiode {} sitter fast i tilstand {}; har blitt påminnet {} ganger siden {}",
            keyValue("vedtaksperiodeId", påminnelse.vedtaksperiodeId),
            keyValue("tilstand", påminnelse.tilstand),
            keyValue("antallGangerPåminnet", påminnelse.antallGangerPåminnet),
            keyValue("endringstidspunkt", påminnelse.endringstidspunkt)
        )

        alertSlack(
            "#team-bømlo-alerts", "spetakkel", String.format(
                "Vedtaksperiode %s sitter fast i tilstand %s. Den er forsøkt påminnet %d ganger siden %s",
                påminnelse.vedtaksperiodeId,
                påminnelse.tilstand,
                påminnelse.antallGangerPåminnet,
                påminnelse.endringstidspunkt.format(ISO_LOCAL_DATE_TIME)
            ),
            ":exclamation:"
        )
    }

    private fun alertSlack(channel: String, username: String, text: String, icon: String) {
        webhookUrl?.post(
            objectMapper.writeValueAsString(
                mapOf(
                    "channel" to channel,
                    "username" to username,
                    "text" to text,
                    "icon_emoji" to icon
                )
            )
        )
    }

    private fun String.post(jsonPayload: String) {
        try {
            (URL(this).openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                doOutput = true
                setRequestProperty("Content-Type", "application/json; charset=utf-8")
                setRequestProperty("User-Agent", "navikt/spetakkel")

                outputStream.use { it.bufferedWriter(Charsets.UTF_8).write(jsonPayload) }
            }.responseCode
        } catch (err: IOException) {
            log.error("feil ved posting til slack: {}", err)
        }
    }

    private class Påminnelse(private val packet: JsonMessage) {
        val vedtaksperiodeId: String get() = packet["vedtaksperiodeId"].asText()
        val tilstand: String get() = packet["tilstand"].asText()
        val endringstidspunkt get() = packet["endringstidspunkt"].asLocalDateTime()
        val antallGangerPåminnet: Int get() = packet["antallGangerPåminnet"].asInt()
    }
}
