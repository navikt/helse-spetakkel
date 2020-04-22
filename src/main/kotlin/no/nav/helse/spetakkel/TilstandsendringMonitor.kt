package no.nav.helse.spetakkel

import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import kotliquery.TransactionalSession
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME
import java.time.temporal.ChronoUnit
import javax.sql.DataSource

class TilstandsendringMonitor(
    rapidsConnection: RapidsConnection,
    private val vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao
) : River.PacketListener {

    private companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val log = LoggerFactory.getLogger(TilstandsendringMonitor::class.java)
        private val tilstandCounter = Counter.build(
            "vedtaksperiode_tilstander_totals",
            "Fordeling av tilstandene periodene er i, og hvilken tilstand de kom fra"
        )
            .labelNames("forrigeTilstand", "tilstand", "hendelse", "timeout")
            .register()
        private val tilstanderGauge = Gauge.build(
            "vedtaksperiode_gjeldende_tilstander",
            "Gjeldende tilstander for vedtaksperioder som ikke har nådd en slutt-tilstand (timeout=0)"
        )
            .labelNames("tilstand")
            .register()
            .apply {
                TilstandType.values().forEach { this.labels(it.name).set(0.0) }
            }
    }

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "vedtaksperiode_endret") }
            validate { it.requireKey("@forårsaket_av.event_name") }
            validate { it.requireKey("aktørId") }
            validate { it.requireKey("fødselsnummer") }
            validate { it.requireKey("organisasjonsnummer") }
            validate { it.requireKey("vedtaksperiodeId") }
            validate { it.requireKey("forrigeTilstand") }
            validate { it.requireKey("gjeldendeTilstand") }
            validate { it.requireKey("@opprettet") }
            validate { it.requireKey("timeout") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val tilstandsendring = VedtaksperiodeTilstandDao.Tilstandsendring(packet)
        sikkerLogg.info(
            "{} endret fra {} til {}:\n{}",
            tilstandsendring.vedtaksperiodeId,
            tilstandsendring.forrigeTilstand,
            tilstandsendring.gjeldendeTilstand,
            packet.toJson()
        )
        refreshCounters(tilstandsendring)

        val historiskTilstandsendring =
            vedtaksperiodeTilstandDao.lagreEllerOppdaterTilstand(tilstandsendring) ?: return

        val diff = historiskTilstandsendring.tidITilstand(tilstandsendring) ?: return

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

    private fun resultat(
        historiskTilstandsendring: VedtaksperiodeTilstandDao.HistoriskTilstandsendring,
        tilstandsendring: VedtaksperiodeTilstandDao.Tilstandsendring,
        diff: Long
    ) = JsonMessage.newMessage(mapOf(
        "@event_name" to "vedtaksperiode_tid_i_tilstand",
        "aktørId" to tilstandsendring.aktørId,
        "fødselsnummer" to tilstandsendring.fødselsnummer,
        "organisasjonsnummer" to tilstandsendring.organisasjonsnummer,
        "vedtaksperiodeId" to tilstandsendring.vedtaksperiodeId,
        "tilstand" to tilstandsendring.forrigeTilstand,
        "nyTilstand" to tilstandsendring.gjeldendeTilstand,
        "starttid" to historiskTilstandsendring.endringstidspunkt,
        "sluttid" to tilstandsendring.endringstidspunkt,
        "timeout" to historiskTilstandsendring.timeout,
        "tid_i_tilstand" to diff
    )).toJson()

    private var lastRefreshTime = LocalDateTime.MIN

    private fun refreshCounters(tilstandsendring: VedtaksperiodeTilstandDao.Tilstandsendring) {
        refreshTilstandGauge()

        tilstandCounter.labels(
            tilstandsendring.forrigeTilstand,
            tilstandsendring.gjeldendeTilstand,
            tilstandsendring.påGrunnAv,
            "${tilstandsendring.timeout}"
        ).inc()

    }

    private fun refreshTilstandGauge() {
        val now = LocalDateTime.now()
        if (lastRefreshTime > now.minusSeconds(30)) return
        log.info("Refreshing tilstand gauge")
        TilstandType.values().forEach { tilstanderGauge.labels(it.name).set(0.0) }
        vedtaksperiodeTilstandDao.hentGjeldendeTilstander().forEach { (tilstand, count) ->
            tilstanderGauge.labels(tilstand).set(count.toDouble())
        }
        lastRefreshTime = now
    }

    class VedtaksperiodeTilstandDao(private val dataSource: DataSource) {

        fun lagreEllerOppdaterTilstand(tilstandsendring: Tilstandsendring): HistoriskTilstandsendring? {
            return using(sessionOf(dataSource)) { session ->
                session.transaction { tx ->
                    hentGjeldendeTilstand(tx, tilstandsendring.vedtaksperiodeId).also {
                        tx.run(
                            queryOf(
                                "INSERT INTO vedtaksperiode_tilstand (vedtaksperiode_id, tilstand, timeout, " +
                                        "endringstidspunkt, endringstidspunkt_nanos) " +
                                        "VALUES (?, ?, ?, ?, ?) " +
                                        "ON CONFLICT (vedtaksperiode_id) DO " +
                                        "UPDATE SET " +
                                        "tilstand=EXCLUDED.tilstand, " +
                                        "timeout = EXCLUDED.timeout, " +
                                        "endringstidspunkt=EXCLUDED.endringstidspunkt, " +
                                        "endringstidspunkt_nanos=EXCLUDED.endringstidspunkt_nanos " +
                                        "WHERE (vedtaksperiode_tilstand.endringstidspunkt < EXCLUDED.endringstidspunkt) " +
                                        "   OR (vedtaksperiode_tilstand.endringstidspunkt = EXCLUDED.endringstidspunkt AND vedtaksperiode_tilstand.endringstidspunkt_nanos < EXCLUDED.endringstidspunkt_nanos)",
                                tilstandsendring.vedtaksperiodeId,
                                tilstandsendring.gjeldendeTilstand,
                                tilstandsendring.timeout,
                                tilstandsendring.endringstidspunkt,
                                tilstandsendring.endringstidspunkt.nano
                            ).asExecute
                        )
                    }
                }
            }
        }

        fun hentGjeldendeTilstander(): Map<String, Long> {
            return using(sessionOf(dataSource)) { session ->
                session.run(queryOf("SELECT tilstand, COUNT(1) FROM vedtaksperiode_tilstand GROUP BY tilstand").map {
                    it.string(1) to it.long(2)
                }.asList)
            }.associate { it }
        }

        private fun hentGjeldendeTilstand(session: TransactionalSession, vedtaksperiodeId: String): HistoriskTilstandsendring? {
            return session.run(
                queryOf(
                    "SELECT vedtaksperiode_id, tilstand, timeout, endringstidspunkt, endringstidspunkt_nanos FROM vedtaksperiode_tilstand " +
                            "WHERE vedtaksperiode_id = ? " +
                            "LIMIT 1", vedtaksperiodeId
                ).map {
                    HistoriskTilstandsendring(
                        tilstand = it.string("tilstand"),
                        timeout = it.long("timeout"),
                        endringstidspunkt = it.localDateTime("@opprettet").withNano(it.int("endringstidspunkt_nanos"))
                    )
                }.asSingle
            )
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
            val endringstidspunkt get() = packet["@opprettet"].asLocalDateTime()
            val påGrunnAv get() = packet["@forårsaket_av.event_name"].asText()
            val timeout: Long get() = packet["timeout"].asLong()
        }
    }

    private enum class TilstandType {
        AVVENTER_HISTORIKK,
        AVVENTER_GODKJENNING,
        AVVENTER_SIMULERING,
        TIL_UTBETALING,
        TIL_INFOTRYGD,
        AVSLUTTET,
        AVSLUTTET_UTEN_UTBETALING,
        UTBETALING_FEILET,
        START,
        MOTTATT_SYKMELDING_FERDIG_FORLENGELSE,
        MOTTATT_SYKMELDING_UFERDIG_FORLENGELSE,
        MOTTATT_SYKMELDING_FERDIG_GAP,
        MOTTATT_SYKMELDING_UFERDIG_GAP,
        AVVENTER_SØKNAD_FERDIG_GAP,
        AVVENTER_SØKNAD_UFERDIG_GAP,
        AVVENTER_VILKÅRSPRØVING_GAP,
        AVVENTER_GAP,
        AVVENTER_INNTEKTSMELDING_FERDIG_GAP,
        AVVENTER_INNTEKTSMELDING_UFERDIG_GAP,
        AVVENTER_UFERDIG_GAP,
        AVVENTER_INNTEKTSMELDING_UFERDIG_FORLENGELSE,
        AVVENTER_SØKNAD_UFERDIG_FORLENGELSE,
        AVVENTER_UFERDIG_FORLENGELSE
    }
}
