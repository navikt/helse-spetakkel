package no.nav.helse.spetakkel

import io.prometheus.client.Counter
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.*
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

internal class GodkjenningsbehovMonitor(rapidsConnection: RapidsConnection, dataSource: DataSource) {
    private companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val godkjenningsbehovløsningCounter =
            Counter.build("godkjenningsbehovlosning_totals", "Antall løste godkjenningsbehov")
                .labelNames(
                    "periodetype",
                    "utbetalingtype",
                    "inntektskilde",
                    "harWarnings",
                    "godkjent",
                    "automatiskBehandling",
                    "forarsaketAvEventName"
                )
                .register()
        private val godkjenningsbehovCounter =
            Counter.build("godkjenningsbehov_totals", "Antall godkjenningsbehov")
                .labelNames(
                    "periodetype",
                    "utbetalingtype",
                    "inntektskilde",
                    "harWarnings",
                    "forarsaketAvEventName",
                    "resendingAvGodkjenningsbehov"
                )
                .register()
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
        override fun onError(problems: MessageProblems, context: MessageContext) {
            sikkerLogg.error("forstod ikke Godkjenningbehovløsning:\n${problems.toExtendedReport()}")
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            godkjenningsbehovløsningCounter.labels(
                packet["Godkjenning.periodetype"].asText(),
                packet["Godkjenning.utbetalingtype"].asText(),
                packet["Godkjenning.inntektskilde"].asText(),
                if (packet["Godkjenning.warnings"].path("aktiviteter")
                        .any { it.path("alvorlighetsgrad").asText() == "WARN" }
                ) "1" else "0",
                if (packet["@løsning.Godkjenning.godkjent"].asBoolean()) "1" else "0",
                if (packet["@løsning.Godkjenning.automatiskBehandling"].asBoolean()) "1" else "0",
                packet["@forårsaket_av.event_name"].asText()
            ).inc()
        }
    }

    private class Godkjenningsbehov(private val dataSource: DataSource) : River.PacketListener {
        override fun onError(problems: MessageProblems, context: MessageContext) {
            sikkerLogg.error("forstod ikke Godkjenningbehov:\n${problems.toExtendedReport()}")
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            godkjenningsbehovCounter.labels(
                packet["Godkjenning.periodetype"].asText(),
                packet["Godkjenning.utbetalingtype"].asText(),
                packet["Godkjenning.inntektskilde"].asText(),
                "0",
                packet["@forårsaket_av.event_name"].asText(),
                if (erOpprinneligGodkjenningsbehov(packet)) "0" else "1",
            ).inc()
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
