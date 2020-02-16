package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

class TilstandsendringMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(TilstandsendringMonitor::class.java)
        private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    private val tilstandsendringer = mutableMapOf<String, HistoriskTilstandsendring>()

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "vedtaksperiode_endret") }
            validate { it.requireKey("aktørId") }
            validate { it.requireKey("fødselsnummer") }
            validate { it.requireKey("organisasjonsnummer") }
            validate { it.requireKey("vedtaksperiodeId") }
            validate { it.requireKey("forrigeTilstand") }
            validate { it.requireKey("gjeldendeTilstand") }
            validate { it.requireKey("endringstidspunkt") }
            validate { it.requireKey("timeout") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val tilstandsendring = Tilstandsendring(packet)

        val historiskTilstandsendring = tilstandsendringer[tilstandsendring.vedtaksperiodeId]
            ?: return tilstandsendringer.lagreTilstandsendring(tilstandsendring)

        // if the one we have is newer, discard current
        if (historiskTilstandsendring.endringstidspunkt > tilstandsendring.endringstidspunkt) return

        tilstandsendringer.lagreTilstandsendring(tilstandsendring)

        val diff = historiskTilstandsendring.tidITilstand(tilstandsendring) ?: return

        log.info(
            "vedtaksperiode {} var i {} i {}",
            keyValue("vedtaksperiodeId", tilstandsendring.vedtaksperiodeId),
            keyValue("tilstand", tilstandsendring.forrigeTilstand),
            String.format(
                "%d dag(er), %d time(r), %d minutt(er) og %d sekund(er)",
                diff / 86400,
                (diff % 86400) / 3600,
                (diff % 3600) / 60,
                diff % 60
            )
        )
        context.send(resultat(tilstandsendring, diff))
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {}

    private fun resultat(tilstandsendring: Tilstandsendring, diff: Long) = objectMapper.writeValueAsString(
        mapOf(
            "@event_name" to "vedtaksperiode_tid_i_tilstand",
            "aktørId" to tilstandsendring.aktørId,
            "fødselsnummer" to tilstandsendring.fødselsnummer,
            "organisasjonsnummer" to tilstandsendring.organisasjonsnummer,
            "vedtaksperiodeId" to tilstandsendring.vedtaksperiodeId,
            "tilstand" to tilstandsendring.forrigeTilstand,
            "tid_i_tilstand" to diff
        )
    )

    private fun MutableMap<String, HistoriskTilstandsendring>.lagreTilstandsendring(tilstandsendring: Tilstandsendring) {
        if (tilstandsendring.timeout == 0L) {
            tilstandsendringer.remove(tilstandsendring.vedtaksperiodeId)
            return
        }

        this[tilstandsendring.vedtaksperiodeId] =
            HistoriskTilstandsendring(tilstandsendring.gjeldendeTilstand, tilstandsendring.endringstidspunkt)
    }

    private class HistoriskTilstandsendring(val tilstand: String, val endringstidspunkt: LocalDateTime) {
        fun tidITilstand(other: Tilstandsendring): Long? {
            // if the one we have is not the previous of the new,
            // we have probably missed an event, so we can't calculate diff
            if (this.tilstand != other.forrigeTilstand) return null
            return ChronoUnit.SECONDS.between(this.endringstidspunkt, other.endringstidspunkt)
        }
    }

    private class Tilstandsendring(private val packet: JsonMessage) {
        val aktørId: String get() = packet["aktørId"].asText()
        val fødselsnummer: String get() = packet["fødselsnummer"].asText()
        val organisasjonsnummer: String get() = packet["organisasjonsnummer"].asText()
        val vedtaksperiodeId: String get() = packet["vedtaksperiodeId"].asText()
        val forrigeTilstand: String get() = packet["forrigeTilstand"].asText()
        val gjeldendeTilstand: String get() = packet["gjeldendeTilstand"].asText()
        val endringstidspunkt get() = packet["endringstidspunkt"].asLocalDateTime()
        val timeout: Long get() = packet["timeout"].asLong()
    }
}
