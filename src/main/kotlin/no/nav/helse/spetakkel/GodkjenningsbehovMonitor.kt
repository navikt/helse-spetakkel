package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.util.*

internal class GodkjenningsbehovMonitor(rapidsConnection: RapidsConnection, godkjenningsbehovDao: GodkjenningsbehovDao) {
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
                    "Godkjenning.warnings",
                    "Godkjenning.periodetype",
                    "Godkjenning.inntektskilde",
                    "@forårsaket_av.event_name",
                    "@forårsaket_av.behov",
                    "vedtaksperiodeId",
                )
            }
        }.register(Godkjenningsbehov(godkjenningsbehovDao))
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

    private class Godkjenningsbehov(private val godkjenningsbehovDao: GodkjenningsbehovDao) : River.PacketListener {
        override fun onError(problems: MessageProblems, context: MessageContext) {
            sikkerLogg.error("forstod ikke Godkjenningbehov:\n${problems.toExtendedReport()}")
        }

        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            populerDuplikatTabell(packet)
            godkjenningsbehovCounter.labels(
                packet["Godkjenning.periodetype"].asText(),
                packet["Godkjenning.utbetalingtype"].asText(),
                packet["Godkjenning.inntektskilde"].asText(),
                if (packet["Godkjenning.warnings"].path("aktiviteter")
                        .any { it.path("alvorlighetsgrad").asText() == "WARN" }
                ) "1" else "0",
                packet["@forårsaket_av.event_name"].asText(),
                if (erOpprinneligGodkjenningsbehov(packet)) "0" else "1",
            ).inc()
        }

        private fun erOpprinneligGodkjenningsbehov(packet: JsonMessage) =
            packet["@forårsaket_av.behov"]
                .map(JsonNode::asText)
                .let { behov -> behov.all { it == "Simulering" } || historikkbehovene.containsAll(behov) }

        private val historikkbehovene: List<String> = listOf(
            "Foreldrepenger",
            "Pleiepenger",
            "Omsorgspenger",
            "Opplæringspenger",
            "Institusjonsopphold",
            "Arbeidsavklaringspenger",
            "Dagpenger",
            "Dødsinfo",
        )

        private fun populerDuplikatTabell(packet: JsonMessage): Boolean {
            val vedtaksperiodeId = packet["vedtaksperiodeId"].asText().let(UUID::fromString)
            return godkjenningsbehovDao.erOpprinneligGodkjenningsbehov(vedtaksperiodeId)
        }

    }
}
