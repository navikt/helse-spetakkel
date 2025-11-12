package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.MultiGauge
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import kotliquery.*
import net.logstash.logback.argument.StructuredArguments.keyValue
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
    }

    init {
        River(rapidsConnection).apply {
            precondition { it.requireValue("@event_name", "vedtaksperiode_endret") }
            validate {
                it.requireKey(
                    "@forårsaket_av", "@forårsaket_av.event_name", "fødselsnummer",
                    "organisasjonsnummer", "vedtaksperiodeId", "forrigeTilstand",
                    "gjeldendeTilstand"
                )
                it.interestedIn("harVedtaksperiodeWarnings")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("makstid", JsonNode::asLocalDateTime)
            }
        }.register(Tilstandsendringer(rapidsConnection, vedtaksperiodeTilstandDao))
        River(rapidsConnection).apply {
            precondition { it.requireValue("@event_name", "person_avstemt") }
            validate {
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
            precondition { it.requireValue("@event_name", "planlagt_påminnelse") }
            validate {
                it.requireKey("vedtaksperiodeId", "tilstand", "endringstidspunkt", "påminnelsetidspunkt", "er_avsluttet")
                it.require("endringstidspunkt", JsonNode::asLocalDateTime)
            }
        }.register(PlanlagtePåminnelser(vedtaksperiodeTilstandDao))
        River(rapidsConnection).apply {
            precondition { it.requireValue("@event_name", "påminnelse") }
            validate {
                it.requireKey("vedtaksperiodeId", "tilstand", "antallGangerPåminnet")
            }
        }.register(Påminnelser(vedtaksperiodeTilstandDao))
    }

    private class Avstemmingsresuiltat(private val rapidsConnection: RapidsConnection, private val vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao) : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
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

        override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
            sikkerLogg.error("forstod ikke vedtaksperiode_endret:\n${problems.toExtendedReport()}")
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
            val tilstandsendring = VedtaksperiodeTilstandDao.Tilstandsendring(packet)
            sikkerLogg.info(
                "{} endret fra {} til {}:\n{}",
                tilstandsendring.vedtaksperiodeId,
                tilstandsendring.forrigeTilstand,
                tilstandsendring.gjeldendeTilstand,
                packet.toJson()
            )
            refreshCounters(meterRegistry, tilstandsendring)

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
            rapidsConnection.publish(
                JsonMessage.newMessage(
                    mapOf(
                        "@event_name" to "vedtaksperiode_i_loop",
                        "@opprettet" to LocalDateTime.now(),
                        "@id" to UUID.randomUUID(),
                        "fødselsnummer" to tilstandsendring.fødselsnummer,
                        "vedtaksperiodeId" to tilstandsendring.vedtaksperiodeId,
                        "forrigeTilstand" to tilstandsendring.forrigeTilstand,
                        "gjeldendeTilstand" to tilstandsendring.gjeldendeTilstand
                    )
                ).toJson()
            )
        }

        private var lastRefreshTime = LocalDateTime.MIN

        private fun refreshCounters(meterRegistry: MeterRegistry, tilstandsendring: VedtaksperiodeTilstandDao.Tilstandsendring) {
            refreshTilstandGauge(meterRegistry)

            Counter.builder("vedtaksperiode_tilstander_totals")
                .description("Fordeling av tilstandene periodene er i, og hvilken tilstand de kom fra")
                .tag("forrigeTilstand", tilstandsendring.forrigeTilstand)
                .tag("tilstand", tilstandsendring.gjeldendeTilstand)
                .tag("hendelse", tilstandsendring.påGrunnAv)
                .tag("harVedtaksperiodeWarnings", if (tilstandsendring.harVedtaksperiodeWarnings) "1" else "0")
                .tag("harHendelseWarnings", if (tilstandsendring.harHendelseWarnings) "1" else "0")
                .register(meterRegistry)
                .increment()

            Counter.builder("vedtaksperiode_warnings_totals")
                .description("Fordeling av warnings per tilstand")
                .tag("tilstand", tilstandsendring.forrigeTilstand)
                .register(meterRegistry)
                .increment()
        }

        private fun refreshTilstandGauge(meterRegistry: MeterRegistry) {
            val now = LocalDateTime.now()
            if (lastRefreshTime > now.minusMinutes(15)) return
            logg.info("Refreshing tilstand gauge")
            val defaultMap = TilstandType.entries.associateWith { 0L }
            val valuesFromDb = vedtaksperiodeTilstandDao.hentGjeldendeTilstander()
                .mapKeys { (navn, _) -> TilstandType.valueOf(navn) }
            refreshTilstandGauge(meterRegistry, defaultMap + valuesFromDb)
            lastRefreshTime = now
        }

        private fun refreshTilstandGauge(meterRegistry: MeterRegistry, tilstander: Map<TilstandType, Long>) {
            MultiGauge.builder("vedtaksperiode_gjeldende_tilstander")
                .description("Gjeldende tilstander for vedtaksperioder som ikke har nådd en slutt-tilstand")
                .register(meterRegistry)
                .register(tilstander.map { (tilstand, antall) ->
                    MultiGauge.Row.of(Tags.of(Tag.of("tilstand", tilstand.name)), antall)
                })
        }
    }

    private class PlanlagtePåminnelser(private val vedtaksperiodeTilstandDao: VedtaksperiodeTilstandDao) : River.PacketListener {
        override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
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
        override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
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

        private fun ignorerTilstandsendringerMellom(tilstandsendring: Tilstandsendring, fraTilstander: Set<String>, tilTilstand: String): Boolean {
            return (tilstandsendring.forrigeTilstand in fraTilstander && tilstandsendring.gjeldendeTilstand == tilTilstand) ||
                    (tilstandsendring.gjeldendeTilstand in fraTilstander && tilstandsendring.forrigeTilstand == tilTilstand)
        }

        fun antallLikeTilstandsendringer(tilstandsendring: Tilstandsendring): Long {
            // bryr oss ikke om perioder som reberegner utbetalingen
            if (ignorerTilstandsendringerMellom(tilstandsendring, setOf("AVVENTER_GODKJENNING", "AVVENTER_SIMULERING"), "AVVENTER_HISTORIKK")) return 0
            if (ignorerTilstandsendringerMellom(tilstandsendring, setOf("AVVENTER_GODKJENNING_REVURDERING", "AVVENTER_SIMULERING_REVURDERING"), "AVVENTER_HISTORIKK_REVURDERING")) return 0

            return sessionOf(dataSource).use { session ->
                val nullstillingstidspunkt = tilstandsendring.endringstidspunkt.minusMinutes(60)

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
        REVURDERING_FEILET,
        UTBETALING_FEILET,
        START,

        AVVENTER_INFOTRYGDHISTORIKK,
        AVVENTER_INNTEKTSMELDING,
        AVVENTER_AVSLUTTET_UTEN_UTBETALING,
        AVVENTER_BLOKKERENDE_PERIODE,
        AVVENTER_A_ORDNINGEN,
        AVVENTER_VILKÅRSPRØVING,
        AVVENTER_HISTORIKK,
        AVVENTER_SIMULERING,
        AVVENTER_GODKJENNING,

        AVVENTER_REVURDERING,
        AVVENTER_HISTORIKK_REVURDERING,
        AVVENTER_VILKÅRSPRØVING_REVURDERING,
        AVVENTER_GODKJENNING_REVURDERING,
        AVVENTER_SIMULERING_REVURDERING,

        AVVENTER_ANNULLERING,

        TIL_UTBETALING,
        TIL_ANNULLERING,
        TIL_INFOTRYGD,
        AVSLUTTET,
        AVSLUTTET_UTEN_UTBETALING,

        FRILANS_START,
        FRILANS_AVVENTER_INFOTRYGDHISTORIKK,
        FRILANS_AVVENTER_BLOKKERENDE_PERIODE,

        ARBEIDSLEDIG_START,
        ARBEIDSLEDIG_AVVENTER_INFOTRYGDHISTORIKK,
        ARBEIDSLEDIG_AVVENTER_BLOKKERENDE_PERIODE,

        SELVSTENDIG_START,
        SELVSTENDIG_AVVENTER_INFOTRYGDHISTORIKK,
        SELVSTENDIG_AVVENTER_BLOKKERENDE_PERIODE,
        SELVSTENDIG_AVVENTER_VILKÅRSPRØVING,
        SELVSTENDIG_AVVENTER_HISTORIKK,
        SELVSTENDIG_AVVENTER_SIMULERING,
        SELVSTENDIG_AVVENTER_GODKJENNING,

        SELVSTENDIG_TIL_UTBETALING,
        SELVSTENDIG_AVSLUTTET
    }
}
