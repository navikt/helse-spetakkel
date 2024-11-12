package no.nav.helse.spetakkel

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

internal class GodkjenningsbehovMonitor(rapidsConnection: RapidsConnection, dataSource: DataSource) {
    private companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandAll("@behov", listOf("Godkjenning"))
                it.demandKey("@løsning")
                it.demandValue("@final", true)
                it.requireAny("Godkjenning.utbetalingtype", listOf("UTBETALING", "REVURDERING"))
                it.interestedIn("Godkjenning.warnings", "Godkjenning.periodetype", "Godkjenning.inntektskilde")
                it.requireKey(
                    "@løsning.Godkjenning.godkjent",
                    "@løsning.Godkjenning.automatiskBehandling",
                    "@forårsaket_av.event_name"
                )
            }
        }.register(Godkjenningsbehovløsninger())
        River(rapidsConnection).apply {
            validate {
                it.demandAll("@behov", listOf("Godkjenning"))
                it.rejectKey("@løsning")
                it.requireAny("Godkjenning.utbetalingtype", listOf("UTBETALING", "REVURDERING"))
                it.requireKey(
                    "Godkjenning.periodetype",
                    "Godkjenning.inntektskilde",
                    "@forårsaket_av.event_name",
                    "vedtaksperiodeId",
                )
            }
        }.register(Godkjenningsbehov(dataSource))
    }

    private class Godkjenningsbehovløsninger : River.PacketListener {
        override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
            sikkerLogg.error("forstod ikke Godkjenningbehovløsning:\n${problems.toExtendedReport()}")
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
            Counter.builder("godkjenningsbehovlosning_totals")
                .description("Antall løste godkjenningsbehov")
                .tag("periodetype", packet["Godkjenning.periodetype"].asText())
                .tag("utbetalingtype", packet["Godkjenning.utbetalingtype"].asText())
                .tag("inntektskilde", packet["Godkjenning.inntektskilde"].asText())
                .tag("harWarnings", if (packet["Godkjenning.warnings"].path("aktiviteter")
                        .any { it.path("alvorlighetsgrad").asText() == "WARN" }
                ) "1" else "0")
                .tag("godkjent", if (packet["@løsning.Godkjenning.godkjent"].asBoolean()) "1" else "0")
                .tag("automatiskBehandling", if (packet["@løsning.Godkjenning.automatiskBehandling"].asBoolean()) "1" else "0")
                .tag("forarsaketAvEventName", packet["@forårsaket_av.event_name"].asText())
                .register(meterRegistry)
                .increment()
        }
    }

    private class Godkjenningsbehov(private val dataSource: DataSource) : River.PacketListener {
        override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
            sikkerLogg.error("forstod ikke Godkjenningbehov:\n${problems.toExtendedReport()}")
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
            Counter.builder("godkjenningsbehov_totals")
                .description("Antall godkjenningsbehov")
                .tag("periodetype", packet["Godkjenning.periodetype"].asText())
                .tag("utbetalingtype", packet["Godkjenning.utbetalingtype"].asText())
                .tag("inntektskilde", packet["Godkjenning.inntektskilde"].asText())
                .tag("harWarnings", "0")
                .tag("forarsaketAvEventName", packet["@forårsaket_av.event_name"].asText())
                .tag("resendingAvGodkjenningsbehov", if (erOpprinneligGodkjenningsbehov(packet)) "0" else "1")
                .register(meterRegistry)
                .increment()
        }


        private fun erOpprinneligGodkjenningsbehov(packet: JsonMessage): Boolean {
            val vedtaksperiodeId = packet["vedtaksperiodeId"].asText().let(UUID::fromString)
            return sessionOf(dataSource).use {
                @Language("PostgreSQL")
                val query = "INSERT INTO vedtaksperiode_godkjenningsbehov_duplikatsjekk (vedtaksperiode_id) VALUES (?) ON CONFLICT DO NOTHING;"
                it.run(
                    queryOf(query, vedtaksperiodeId).asUpdate
                ) > 0
            }
        }

    }
}
