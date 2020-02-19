package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.prometheus.client.Histogram
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME
import java.time.temporal.ChronoUnit
import javax.sql.DataSource

class TilstandsendringMonitor(
    rapidsConnection: RapidsConnection,
    private val vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao
) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(TilstandsendringMonitor::class.java)
        private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

        private val histogram = Histogram.build(
            "vedtaksperiode_tilstand_latency_seconds",
            "Antall sekunder en vedtaksperiode er i en tilstand"
        )
            .labelNames("tilstand")
            .buckets(1.minute, 1.hours, 12.hours, 24.hours, 7.days, 30.days)
            .register()

        private val Int.minute get() = Duration.ofMinutes(this.toLong()).toDouble()
        private val Int.hours get() = Duration.ofHours(this.toLong()).toDouble()
        private val Int.days get() = Duration.ofDays(this.toLong()).toDouble()
        private fun Duration.toDouble() = this.toSeconds().toDouble()
    }

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
        val tilstandsendring = VedtaksperiodeTilstandDao.Tilstandsendring(packet)

        val historiskTilstandsendring =
            vedtaksperiodeTilstandDao.hentGjeldendeTilstand(tilstandsendring.vedtaksperiodeId)
                ?: return vedtaksperiodeTilstandDao.lagreTilstandsendring(tilstandsendring)

        // if the one we have is newer, discard current
        if (historiskTilstandsendring.endringstidspunkt > tilstandsendring.endringstidspunkt) return

        vedtaksperiodeTilstandDao.oppdaterTilstandsendring(tilstandsendring)

        val diff = historiskTilstandsendring.tidITilstand(tilstandsendring) ?: return

        histogram.labels(historiskTilstandsendring.tilstand)
            .observe(diff.toDouble())
        log.info(
            "vedtaksperiode {} var i {} i {} ({}); gikk til {} {}",
            keyValue("vedtaksperiodeId", tilstandsendring.vedtaksperiodeId),
            keyValue("tilstand", tilstandsendring.forrigeTilstand),
            humanReadableTime(diff),
            historiskTilstandsendring.endringstidspunkt.format(ISO_LOCAL_DATE_TIME),
            tilstandsendring.gjeldendeTilstand,
            tilstandsendring.endringstidspunkt.format(ISO_LOCAL_DATE_TIME)
        )
        context.send(resultat(historiskTilstandsendring, tilstandsendring, diff))
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {}

    private fun resultat(historiskTilstandsendring: VedtaksperiodeTilstandDao.HistoriskTilstandsendring, tilstandsendring: VedtaksperiodeTilstandDao.Tilstandsendring, diff: Long) = objectMapper.writeValueAsString(
        mapOf(
            "@event_name" to "vedtaksperiode_tid_i_tilstand",
            "aktørId" to tilstandsendring.aktørId,
            "fødselsnummer" to tilstandsendring.fødselsnummer,
            "organisasjonsnummer" to tilstandsendring.organisasjonsnummer,
            "vedtaksperiodeId" to tilstandsendring.vedtaksperiodeId,
            "tilstand" to tilstandsendring.forrigeTilstand,
            "timeout" to historiskTilstandsendring.timeout,
            "tid_i_tilstand" to diff
        )
    )

    class VedtaksperiodeTilstandDao(private val dataSource: DataSource) {

        fun hentGjeldendeTilstand(vedtaksperiodeId: String): HistoriskTilstandsendring? {
            return using(sessionOf(dataSource)) { session ->
                session.run(
                    queryOf(
                        "SELECT vedtaksperiode_id, tilstand, timeout, endringstidspunkt FROM vedtaksperiode_tilstand " +
                                "WHERE vedtaksperiode_id = ? " +
                                "LIMIT 1", vedtaksperiodeId
                    ).map {
                        HistoriskTilstandsendring(
                            tilstand = it.string("tilstand"),
                            timeout = it.long("timeout"),
                            endringstidspunkt = it.localDateTime("endringstidspunkt")
                        )
                    }.asSingle
                )
            }
        }

        fun lagreTilstandsendring(tilstandsendring: Tilstandsendring) {
            using(sessionOf(dataSource)) { session ->
                if (tilstandsendring.timeout == 0L) return@using

                session.run(
                    queryOf(
                        "INSERT INTO vedtaksperiode_tilstand (vedtaksperiode_id, tilstand, timeout, endringstidspunkt) VALUES (?, ?, ?, ?)",
                        tilstandsendring.vedtaksperiodeId,
                        tilstandsendring.gjeldendeTilstand,
                        tilstandsendring.timeout,
                        tilstandsendring.endringstidspunkt
                    ).asExecute
                )
            }
        }

        fun oppdaterTilstandsendring(tilstandsendring: Tilstandsendring) {
            using(sessionOf(dataSource)) { session ->
                if (tilstandsendring.timeout == 0L) {
                    session.run(
                        queryOf(
                            "DELETE FROM vedtaksperiode_tilstand WHERE vedtaksperiode_id=?",
                            tilstandsendring.vedtaksperiodeId
                        ).asExecute
                    )
                } else {
                    session.run(
                        queryOf(
                            "UPDATE vedtaksperiode_tilstand SET tilstand=?, timeout = ?, endringstidspunkt=? WHERE vedtaksperiode_id=?",
                            tilstandsendring.gjeldendeTilstand,
                            tilstandsendring.timeout,
                            tilstandsendring.endringstidspunkt,
                            tilstandsendring.vedtaksperiodeId
                        ).asExecute
                    )
                }
            }
        }

        class HistoriskTilstandsendring(val tilstand: String, val timeout: Long, val endringstidspunkt: LocalDateTime) {
            fun tidITilstand(other: Tilstandsendring): Long? {
                // if the one we have is not the previous of the new,
                // we have probably missed an event, so we can't calculate diff
                if (this.tilstand != other.forrigeTilstand) return null
                return ChronoUnit.SECONDS.between(this.endringstidspunkt, other.endringstidspunkt)
            }
        }

        class Tilstandsendring(private val packet: JsonMessage) {
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
}
