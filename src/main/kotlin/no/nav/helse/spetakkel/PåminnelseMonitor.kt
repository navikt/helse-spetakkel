package no.nav.helse.spetakkel

import com.bazaarvoice.jackson.rison.RisonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URL
import java.time.LocalDateTime
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

        private val risonMapper = ObjectMapper(RisonFactory())
    }

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "påminnelse") }
            validate { it.requireKey("vedtaksperiodeId") }
            validate { it.requireKey("antallGangerPåminnet") }
            validate { it.requireKey("tilstandsendringstidspunkt") }
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
            "{} sitter fast i {}; har blitt påminnet {} ganger siden {}",
            keyValue("vedtaksperiodeId", påminnelse.vedtaksperiodeId),
            keyValue("tilstand", påminnelse.tilstand),
            keyValue("antallGangerPåminnet", påminnelse.antallGangerPåminnet),
            keyValue("tilstandsendringstidspunkt", påminnelse.endringstidspunkt.format(ISO_LOCAL_DATE_TIME))
        )

        alertSlack(
            "#team-bømlo-alerts", "spetakkel", String.format(
                "Vedtaksperiode <%s|%s> (<%s|tjenestekall>) sitter fast i tilstand %s. Den er forsøkt påminnet %d ganger siden %s",
                kibanaLink("logstash-apps-*", påminnelse.vedtaksperiodeId, påminnelse.endringstidspunkt, LocalDateTime.now()),
                påminnelse.vedtaksperiodeId,
                kibanaLink("tjenestekall-*", påminnelse.vedtaksperiodeId, påminnelse.endringstidspunkt, LocalDateTime.now()),
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
        ) ?: log.info("not alerting slack because URL is not set")
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

            if (connection.responseCode in 200..299) {
                log.info("response from slack: code=$responseCode body=${connection.inputStream.readText()}")
            } else {
                log.error("response from slack: code=$responseCode body=${connection.errorStream.readText()}")
            }
        } catch (err: IOException) {
            log.error("feil ved posting til slack: {}", err)
        }
    }

    private fun kibanaLink(
        index: String = "logstash-apps-*",
        vedtaksperiodeId: String,
        starttidspunkt: LocalDateTime,
        sluttidspunkt: LocalDateTime
    ): String {
        val urlFormat = "https://logs.adeo.no/app/kibana#/discover?_a=%s&_g=%s"
        val searchQuery = "\"%s\""

        val appState = mapOf(
            "index" to index,
            "query" to mapOf(
                "language" to "lucene",
                "query" to String.format(searchQuery, vedtaksperiodeId)
            )
        )

        val globalState = mapOf(
            "time" to mapOf(
                "from" to starttidspunkt.format(ISO_LOCAL_DATE_TIME),
                "mode" to "absolute",
                "to" to sluttidspunkt.format(ISO_LOCAL_DATE_TIME)
            )
        )

        return String.format(
            urlFormat,
            risonMapper.writeValueAsString(appState),
            risonMapper.writeValueAsString(globalState)
        )
    }

    private fun InputStream.readText() = use { it.bufferedReader().readText() }

    private class Påminnelse(private val packet: JsonMessage) {
        val vedtaksperiodeId: String get() = packet["vedtaksperiodeId"].asText()
        val tilstand: String get() = packet["tilstand"].asText()
        val endringstidspunkt get() = packet["tilstandsendringstidspunkt"].asLocalDateTime()
        val antallGangerPåminnet: Int get() = packet["antallGangerPåminnet"].asInt()
    }
}
