package no.nav.helse.spetakkel

import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory

internal class GodkjenningsbehovMonitor(rapidsConnection: RapidsConnection) {
    private companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val godkjenningsbehovløsningCounter =
            Counter.build("godkjenningsbehovlosning_totals", "Antall løste godkjenningsbehov")
                    .labelNames("periodetype", "harWarnings", "godkjent", "automatiskBehandling")
                .register()
        private val godkjenningsbehovCounter =
            Counter.build("godkjenningsbehov_totals", "Antall godkjenningsbehov")
                    .labelNames("periodetype", "harWarnings")
                .register()
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandAll("@behov", listOf("Godkjenning"))
                it.demandKey("@løsning")
                it.demandValue("@final", true)
                it.requireKey("warnings", "periodetype")
                it.requireKey("@løsning.Godkjenning.godkjent", "@løsning.Godkjenning.automatiskBehandling")
            }
        }.register(Godkjenningsbehovløsninger())
        River(rapidsConnection).apply {
            validate {
                it.demandAll("@behov", listOf("Godkjenning"))
                it.rejectKey("@løsning")
                it.requireKey("warnings", "periodetype")
            }
        }.register(Godkjenningsbehov())
    }

    private class Godkjenningsbehov() : River.PacketListener {
        override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
            sikkerLogg.error("forstod ikke Godkjenningbehov:\n${problems.toExtendedReport()}")
        }

        override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
            godkjenningsbehovCounter.labels(
                    packet["periodetype"].asText(),
                    if (packet["warnings"].path("aktiviteter").any { it.path("alvorlighetsgrad").asText() == "WARN" }) "1" else "0"
            ).inc()
        }
    }

    private class Godkjenningsbehovløsninger() : River.PacketListener {
        override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
            sikkerLogg.error("forstod ikke Godkjenningbehovløsning:\n${problems.toExtendedReport()}")
        }

        override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
            godkjenningsbehovløsningCounter.labels(
                    packet["periodetype"].asText(),
                    if (packet["warnings"].path("aktiviteter").any { it.path("alvorlighetsgrad").asText() == "WARN" }) "1" else "0",
                    if (packet["@løsning.Godkjenning.godkjent"].asBoolean()) "1" else "0",
                    if (packet["@løsning.Godkjenning.automatiskBehandling"].asBoolean()) "1" else "0"
            ).inc()
        }
    }
}
