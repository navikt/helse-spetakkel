package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import kotliquery.TransactionalSession
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME
import java.time.temporal.ChronoUnit
import javax.sql.DataSource

class TilstandsendringMonitor(
    rapidsConnection: RapidsConnection,
    vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao
) {

    private companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val log = LoggerFactory.getLogger(TilstandsendringMonitor::class.java)
        private val tilstandCounter = Counter.build(
            "vedtaksperiode_tilstander_totals",
            "Fordeling av tilstandene periodene er i, og hvilken tilstand de kom fra"
        )
            .labelNames("forrigeTilstand", "tilstand", "hendelse", "harVedtaksperiodeWarnings", "harHendelseWarnings")
            .register()
        private val tilstandWarningsCounter = Counter.build(
            "vedtaksperiode_warnings_totals",
            "Fordeling av warnings per tilstand"
        )
            .labelNames("tilstand")
            .register()
        private val tilstanderGauge = Gauge.build(
            "vedtaksperiode_gjeldende_tilstander",
            "Gjeldende tilstander for vedtaksperioder som ikke har nådd en slutt-tilstand"
        )
            .labelNames("tilstand")
            .register()
            .apply {
                TilstandType.values().forEach { this.labels(it.name).set(0.0) }
            }
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_endret")
                it.requireKey("@forårsaket_av", "@forårsaket_av.event_name", "aktørId", "fødselsnummer",
                    "organisasjonsnummer", "vedtaksperiodeId", "forrigeTilstand",
                    "gjeldendeTilstand", "vedtaksperiode_aktivitetslogg.aktiviteter", "aktivitetslogg.aktiviteter")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("makstid", JsonNode::asLocalDateTime)
            }
        }.register(Tilstandsendringer(vedtaksperiodeTilstandDao))
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "planlagt_påminnelse")
                it.requireKey("vedtaksperiodeId", "tilstand", "endringstidspunkt", "påminnelsetidspunkt", "er_avsluttet")
                it.require("endringstidspunkt", JsonNode::asLocalDateTime)
            }
        }.register(PlanlagtePåminnelser(vedtaksperiodeTilstandDao))
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "påminnelse")
                it.requireKey("vedtaksperiodeId", "tilstand", "antallGangerPåminnet")
            }
        }.register(Påminnelser(vedtaksperiodeTilstandDao))
    }

    private class Tilstandsendringer(private val vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao) : River.PacketListener {

        override fun onError(problems: MessageProblems, context: MessageContext) {
            sikkerLogg.error("forstod ikke vedtaksperiode_endret:\n${problems.toExtendedReport()}")
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext) {
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
            context.publish(
                JsonMessage.newMessage(
                    mapOf(
                        "@event_name" to "vedtaksperiode_tid_i_tilstand",
                        "aktørId" to tilstandsendring.aktørId,
                        "fødselsnummer" to tilstandsendring.fødselsnummer,
                        "organisasjonsnummer" to tilstandsendring.organisasjonsnummer,
                        "vedtaksperiodeId" to tilstandsendring.vedtaksperiodeId,
                        "tilstand" to tilstandsendring.forrigeTilstand,
                        "nyTilstand" to tilstandsendring.gjeldendeTilstand,
                        "starttid" to historiskTilstandsendring.endringstidspunkt,
                        "sluttid" to tilstandsendring.endringstidspunkt,
                        "antall_paminnelser" to historiskTilstandsendring.antallPåminnelser,
                        "makstid" to historiskTilstandsendring.makstid,
                        "timeout_første_påminnelse" to historiskTilstandsendring.timeout,
                        "endret_tilstand_på_grunn_av" to packet["@forårsaket_av"],
                        "tid_i_tilstand" to diff
                    )
                ).toJson().also {
                    sikkerLogg.info("sender event=vedtaksperiode_tid_i_tilstand for {} i {}:\n\t{}",
                        keyValue("vedtaksperiodeId", tilstandsendring.vedtaksperiodeId),
                        keyValue("tilstand", tilstandsendring.forrigeTilstand),
                        it
                    )
                }
            )
        }

        private var lastRefreshTime = LocalDateTime.MIN

        private fun refreshCounters(tilstandsendring: VedtaksperiodeTilstandDao.Tilstandsendring) {
            refreshTilstandGauge()

            tilstandCounter.labels(
                tilstandsendring.forrigeTilstand,
                tilstandsendring.gjeldendeTilstand,
                tilstandsendring.påGrunnAv,
                if (tilstandsendring.harVedtaksperiodeWarnings) "1" else "0",
                if (tilstandsendring.harHendelseWarnings) "1" else "0"
            ).inc()

            tilstandWarningsCounter
                .labels(tilstandsendring.forrigeTilstand)
                .inc(tilstandsendring.antallHendelseWarnings.toDouble())
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
    }

    private class PlanlagtePåminnelser(private val vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao) : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            val timeout = if (packet["er_avsluttet"].asBoolean()) 0 else ChronoUnit.SECONDS.between(
                packet["endringstidspunkt"].asLocalDateTime(),
                packet["påminnelsetidspunkt"].asLocalDateTime()
            )
            vedtaksperiodeTilstandDao.oppdaterTimeout(
                vedtaksperiodeId = packet["vedtaksperiodeId"].asText(),
                tilstand = packet["tilstand"].asText(),
                timeout = timeout
            )
        }
    }

    private class Påminnelser(private val vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao) : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            vedtaksperiodeTilstandDao.oppdaterAntallPåminnelser(
                vedtaksperiodeId = packet["vedtaksperiodeId"].asText(),
                tilstand = packet["tilstand"].asText(),
                antallPåminnelser = packet["antallGangerPåminnet"].asInt()
            )
        }
    }

    class VedtaksperiodeTilstandDao(private val dataSource: DataSource) {

        fun lagreEllerOppdaterTilstand(tilstandsendring: Tilstandsendring): HistoriskTilstandsendring? {
            return using(sessionOf(dataSource)) { session ->
                session.transaction { tx ->
                    hentGjeldendeTilstand(tx, tilstandsendring.vedtaksperiodeId).also {
                        tx.run(
                            queryOf(
                                "INSERT INTO vedtaksperiode_tilstand (vedtaksperiode_id, tilstand, makstid, " +
                                        "endringstidspunkt, endringstidspunkt_nanos) " +
                                        "VALUES (?, ?, ?, ?, ?) " +
                                        "ON CONFLICT (vedtaksperiode_id) DO " +
                                        "UPDATE SET " +
                                        "tilstand=EXCLUDED.tilstand, " +
                                        "timeout=0, " +
                                        "makstid=EXCLUDED.makstid, " +
                                        "antall_paminnelser=0, " +
                                        "endringstidspunkt=EXCLUDED.endringstidspunkt, " +
                                        "endringstidspunkt_nanos=EXCLUDED.endringstidspunkt_nanos " +
                                        "WHERE (vedtaksperiode_tilstand.endringstidspunkt < EXCLUDED.endringstidspunkt) " +
                                        "   OR (vedtaksperiode_tilstand.endringstidspunkt = EXCLUDED.endringstidspunkt AND vedtaksperiode_tilstand.endringstidspunkt_nanos < EXCLUDED.endringstidspunkt_nanos)",
                                tilstandsendring.vedtaksperiodeId,
                                tilstandsendring.gjeldendeTilstand,
                                tilstandsendring.makstid,
                                tilstandsendring.endringstidspunkt,
                                tilstandsendring.endringstidspunkt.nano
                            ).asExecute
                        )
                    }
                }
            }
        }

        fun oppdaterTimeout(vedtaksperiodeId: String, tilstand: String, timeout: Long) {
            using(sessionOf(dataSource)) {
                it.run(queryOf("UPDATE vedtaksperiode_tilstand SET timeout = ? WHERE vedtaksperiode_id = ? AND tilstand = ?",
                    timeout, vedtaksperiodeId, tilstand).asExecute)
            }
        }

        fun oppdaterAntallPåminnelser(vedtaksperiodeId: String, tilstand: String, antallPåminnelser: Int) {
            using(sessionOf(dataSource)) {
                it.run(queryOf("UPDATE vedtaksperiode_tilstand SET antall_paminnelser = ? WHERE vedtaksperiode_id = ? AND tilstand = ?",
                    antallPåminnelser, vedtaksperiodeId, tilstand).asExecute)
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
                    "SELECT vedtaksperiode_id, tilstand, antall_paminnelser, timeout, makstid, endringstidspunkt, endringstidspunkt_nanos FROM vedtaksperiode_tilstand " +
                            "WHERE vedtaksperiode_id = ? " +
                            "LIMIT 1", vedtaksperiodeId
                ).map {
                    HistoriskTilstandsendring(
                        tilstand = it.string("tilstand"),
                        antallPåminnelser = it.int("antall_paminnelser"),
                        timeout = it.long("timeout"),
                        makstid = it.localDateTime("makstid"),
                        endringstidspunkt = it.localDateTime("endringstidspunkt").withNano(it.int("endringstidspunkt_nanos"))
                    )
                }.asSingle
            )
        }

        class HistoriskTilstandsendring(
            val tilstand: String,
            val antallPåminnelser: Int,
            val timeout: Long,
            makstid: LocalDateTime,
            val endringstidspunkt: LocalDateTime
        ) {
            val makstid = makstid.takeUnless { it.year == 9999 } ?: LocalDateTime.MAX

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
            val makstid get() = packet["makstid"]
                .asLocalDateTime()
                .takeUnless { it == LocalDateTime.MAX }
                ?: LocalDateTime.of(LocalDate.ofYearDay(9999, 1), LocalTime.MIN)
            val påGrunnAv get() = packet["@forårsaket_av.event_name"].asText()
            val harVedtaksperiodeWarnings
                get() = packet["vedtaksperiode_aktivitetslogg.aktiviteter"]
                    .takeIf(JsonNode::isArray)
                    ?.filter { it["alvorlighetsgrad"].asText() == "WARN" }
                    ?.isNotEmpty() ?: false
            val antallHendelseWarnings
                get() = packet["aktivitetslogg.aktiviteter"]
                    .takeIf(JsonNode::isArray)
                    ?.filter { it["alvorlighetsgrad"].asText() == "WARN" }
                    ?.count() ?: 0
            val harHendelseWarnings get() = antallHendelseWarnings > 0
        }
    }

    private enum class TilstandType {
        AVVENTER_HISTORIKK,
        AVVENTER_GODKJENNING,
        AVVENTER_SIMULERING,
        TIL_UTBETALING,
        TIL_ANNULLERING,
        TIL_INFOTRYGD,
        AVSLUTTET,
        AVSLUTTET_UTEN_UTBETALING,
        AVSLUTTET_UTEN_UTBETALING_MED_INNTEKTSMELDING,
        UTEN_UTBETALING_MED_INNTEKTSMELDING_UFERDIG_GAP,
        UTEN_UTBETALING_MED_INNTEKTSMELDING_UFERDIG_FORLENGELSE,
        UTBETALING_FEILET,
        START,
        MOTTATT_SYKMELDING_FERDIG_FORLENGELSE,
        MOTTATT_SYKMELDING_UFERDIG_FORLENGELSE,
        MOTTATT_SYKMELDING_FERDIG_GAP,
        MOTTATT_SYKMELDING_UFERDIG_GAP,
        AVVENTER_SØKNAD_FERDIG_GAP,
        AVVENTER_ARBEIDSGIVERSØKNAD_FERDIG_GAP,
        AVVENTER_ARBEIDSGIVERSØKNAD_UFERDIG_GAP,
        AVVENTER_SØKNAD_UFERDIG_GAP,
        AVVENTER_VILKÅRSPRØVING_GAP,
        AVVENTER_VILKÅRSPRØVING_ARBEIDSGIVERSØKNAD,
        AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP,
        AVVENTER_INNTEKTSMELDING_UFERDIG_GAP,
        AVVENTER_UFERDIG_GAP,
        AVVENTER_INNTEKTSMELDING_UFERDIG_FORLENGELSE,
        AVVENTER_SØKNAD_UFERDIG_FORLENGELSE,
        AVVENTER_UFERDIG_FORLENGELSE,
        AVVENTER_ARBEIDSGIVERE
    }
}
