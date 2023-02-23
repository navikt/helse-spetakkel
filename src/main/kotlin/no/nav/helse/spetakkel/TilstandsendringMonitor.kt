package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import kotliquery.*
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME
import java.time.temporal.ChronoUnit
import java.util.*
import javax.sql.DataSource

class TilstandsendringMonitor(
    val rapidsConnection: RapidsConnection,
    vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao
) {

    private companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val logg = LoggerFactory.getLogger(TilstandsendringMonitor::class.java)
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
                it.requireKey(
                    "@forårsaket_av", "@forårsaket_av.event_name", "aktørId", "fødselsnummer",
                    "organisasjonsnummer", "vedtaksperiodeId", "forrigeTilstand",
                    "gjeldendeTilstand"
                )
                it.interestedIn("harVedtaksperiodeWarnings")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("makstid", JsonNode::asLocalDateTime)
            }
        }.register(Tilstandsendringer(rapidsConnection, vedtaksperiodeTilstandDao))
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "person_avstemt")
                it.requireKey("fødselsnummer")
                it.requireArray("arbeidsgivere") {
                    requireArray("vedtaksperioder") {
                        requireKey("id", "tilstand")
                        require("oppdatert", JsonNode::asLocalDateTime)
                    }
                    requireArray("forkastedeVedtaksperioder") {
                        requireKey("id", "tilstand")
                        require("oppdatert", JsonNode::asLocalDateTime)
                    }
                }
            }
        }.register(Avstemmingsresuiltat(rapidsConnection, vedtaksperiodeTilstandDao))
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

    private class Avstemmingsresuiltat(private val rapidsConnection: RapidsConnection, private val vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao) : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            val antallEndringer = packet["arbeidsgivere"].sumOf { arbeidsgiver ->
                arbeidsgiver.path("vedtaksperioder").filter { vedtaksperiode ->
                    vedtaksperiodeTilstandDao.lagreEllerOppdaterTilstand(
                        vedtaksperiode.path("id").asText(),
                        vedtaksperiode.path("tilstand").asText(),
                        vedtaksperiode.path("oppdatert").asLocalDateTime()
                    )
                }.size + arbeidsgiver.path("forkastedeVedtaksperioder").filter { vedtaksperiode ->
                    vedtaksperiodeTilstandDao.lagreEllerOppdaterTilstand(
                        vedtaksperiode.path("id").asText(),
                        vedtaksperiode.path("tilstand").asText(),
                        vedtaksperiode.path("oppdatert").asLocalDateTime()
                    )
                }.size
            }

            if (antallEndringer == 0) return
            sikkerLogg.info("Oppdaterte tilstand på $antallEndringer vedtaksperioder for person ${packet["fødselsnummer"].asText()}")
        }
    }

    private class Tilstandsendringer(private val rapidsConnection: RapidsConnection, private val vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao) :
        River.PacketListener {

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
            loopDetection(tilstandsendring)

            val diff = historiskTilstandsendring.tidITilstand(tilstandsendring) ?: return

            logg.info(
                "vedtaksperiode {} var i {} i {} ({}); gikk til {} {}",
                keyValue("vedtaksperiodeId", tilstandsendring.vedtaksperiodeId),
                keyValue("tilstand", tilstandsendring.forrigeTilstand),
                humanReadableTime(diff),
                historiskTilstandsendring.endringstidspunkt.format(ISO_LOCAL_DATE_TIME),
                tilstandsendring.gjeldendeTilstand,
                tilstandsendring.endringstidspunkt.format(ISO_LOCAL_DATE_TIME)
            )
        }

        private fun loopDetection(tilstandsendring: VedtaksperiodeTilstandDao.Tilstandsendring) {
            if (vedtaksperiodeTilstandDao.antallLikeTilstandsendringer(tilstandsendring) < 10) return
            if (tilstandsendring.forrigeTilstand in listOf(
                    "AVVENTER_GODKJENNING",
                    "AVVENTER_SIMULERING"
                ) && tilstandsendring.gjeldendeTilstand == "AVVENTER_HISTORIKK"
            ) return
            if (tilstandsendring.gjeldendeTilstand in listOf(
                    "AVVENTER_GODKJENNING",
                    "AVVENTER_SIMULERING"
                ) && tilstandsendring.forrigeTilstand == "AVVENTER_HISTORIKK"
            ) return
            if (tilstandsendring.gjeldendeTilstand == "AVSLUTTET_UTEN_UTBETALING"
                && tilstandsendring.forrigeTilstand == "AVSLUTTET_UTEN_UTBETALING"
            ) return
            rapidsConnection.publish(
                JsonMessage.newMessage(
                    mapOf(
                        "@event_name" to "vedtaksperiode_i_loop",
                        "@opprettet" to LocalDateTime.now(),
                        "@id" to UUID.randomUUID(),
                        "vedtaksperiodeId" to tilstandsendring.vedtaksperiodeId,
                        "forrigeTilstand" to tilstandsendring.forrigeTilstand,
                        "gjeldendeTilstand" to tilstandsendring.gjeldendeTilstand
                    )
                ).toJson()
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
            if (lastRefreshTime > now.minusMinutes(15)) return
            logg.info("Refreshing tilstand gauge")
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
                                "INSERT INTO vedtaksperiode_endret_historikk (vedtaksperiode_id, forrige, gjeldende, endringstidspunkt, endringstidspunkt_nanos) " +
                                        "VALUES (?::uuid, ?, ?, ?, ?) " +
                                        "ON CONFLICT(endringstidspunkt,endringstidspunkt_nanos) DO NOTHING",
                                tilstandsendring.vedtaksperiodeId,
                                tilstandsendring.forrigeTilstand,
                                tilstandsendring.gjeldendeTilstand,
                                tilstandsendring.endringstidspunkt,
                                tilstandsendring.endringstidspunkt.nano
                            ).asExecute
                        )
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

        fun lagreEllerOppdaterTilstand(vedtaksperiodeId: String, tilstand: String, tidspunkt: LocalDateTime): Boolean {
            return using(sessionOf(dataSource)) { session ->
                session.transaction { tx ->
                    val gjeldende = hentGjeldendeTilstand(tx, vedtaksperiodeId)
                    if (gjeldende != null && gjeldende.tilstand == tilstand) return@transaction false
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
                            vedtaksperiodeId,
                            tilstand,
                            LocalDateTime.of(LocalDate.ofYearDay(9999, 1), LocalTime.MIN),
                            tidspunkt,
                            tidspunkt.nano
                        ).asExecute
                    )
                    true
                }
            }
        }

        fun oppdaterTimeout(vedtaksperiodeId: String, tilstand: String, timeout: Long) {
            using(sessionOf(dataSource)) {
                it.run(
                    queryOf(
                        "UPDATE vedtaksperiode_tilstand SET timeout = ? WHERE vedtaksperiode_id = ? AND tilstand = ?",
                        timeout, vedtaksperiodeId, tilstand
                    ).asExecute
                )
            }
        }

        fun oppdaterAntallPåminnelser(vedtaksperiodeId: String, tilstand: String, antallPåminnelser: Int) {
            using(sessionOf(dataSource)) {
                it.run(
                    queryOf(
                        "UPDATE vedtaksperiode_tilstand SET antall_paminnelser = ? WHERE vedtaksperiode_id = ? AND tilstand = ?",
                        antallPåminnelser, vedtaksperiodeId, tilstand
                    ).asExecute
                )
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

        fun antallLikeTilstandsendringer(tilstandsendring: Tilstandsendring): Long {
            return using(sessionOf(dataSource)) { session ->
                val nullstillingstidspunkt =
                    finnForrigeRevurderingstidspunkt(session, tilstandsendring) ?: LocalDateTime.of(1814, 5, 17, 11, 0)

                @Language("PostgreSQL")
                val statement = """
                    SELECT count(1) as antall
                    FROM vedtaksperiode_endret_historikk
                    WHERE vedtaksperiode_id = ?::uuid
                      AND gjeldende = ?
                      AND forrige = ?
                      AND endringstidspunkt > ?
                """
                session.run(
                    queryOf(
                        statement,
                        tilstandsendring.vedtaksperiodeId,
                        tilstandsendring.gjeldendeTilstand,
                        tilstandsendring.forrigeTilstand,
                        nullstillingstidspunkt,
                    ).map { it.long("antall") }.asSingle
                )
            }!!
        }

        private fun finnForrigeRevurderingstidspunkt(
            session: Session,
            tilstandsendring: Tilstandsendring,
        ): LocalDateTime? {
            @Language("PostgreSQL")
            val statement = """
                SELECT endringstidspunkt
                FROM vedtaksperiode_endret_historikk
                WHERE vedtaksperiode_id = ?::UUID
                    AND gjeldende = 'AVVENTER_HISTORIKK_REVURDERING'
                ORDER BY endringstidspunkt desc
                LIMIT 1
            """
            return session.run(
                queryOf(
                    statement,
                    tilstandsendring.vedtaksperiodeId,
                ).map { it.localDateTime("endringstidspunkt") }.asSingle
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
            val makstid
                get() = packet["makstid"]
                    .asLocalDateTime()
                    .takeUnless { it == LocalDateTime.MAX }
                    ?: LocalDateTime.of(LocalDate.ofYearDay(9999, 1), LocalTime.MIN)
            val påGrunnAv get() = packet["@forårsaket_av.event_name"].asText()
            val antallHendelseWarnings = 0
            val harVedtaksperiodeWarnings get() = packet["harVedtaksperiodeWarnings"].takeIf(JsonNode::isBoolean)?.booleanValue() ?: false
            val harHendelseWarnings get() = antallHendelseWarnings > 0
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
        REVURDERING_FEILET,
        UTBETALING_FEILET,
        START,
        AVVENTER_VILKÅRSPRØVING,
        AVVENTER_REVURDERING,
        AVVENTER_GJENNOMFØRT_REVURDERING,
        AVVENTER_HISTORIKK_REVURDERING,
        AVVENTER_VILKÅRSPRØVING_REVURDERING,
        AVVENTER_SIMULERING_REVURDERING,
        AVVENTER_GODKJENNING_REVURDERING,
        AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK,
        AVVENTER_BLOKKERENDE_PERIODE,
        AVVENTER_INFOTRYGDHISTORIKK
    }
}
