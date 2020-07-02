package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory

class ForlengelserUtenAdvarslerMonitor(
        rapidsConnection: RapidsConnection
) {

    private companion object {
        private val log = LoggerFactory.getLogger(TilstandsendringMonitor::class.java)

        private val counter = Counter.build(
                "forlengelser_til_godkjenning_uten_advarsler",
                "Antall perioder som ikke er førstegangsbehandling og ikke har noen warnings"
        ).register()
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "vedtaksperiode_endret")
                it.requireValue("gjeldendeTilstand", "AVVENTER_GODKJENNING")
                it.requireKey("aktivitetslogg.aktiviteter")
            }
        }.register(TilGodkjenning())
    }

    private class TilGodkjenning : River.PacketListener {

        override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
            if (erForlengelse(packet)) {
                log.info("Forlengelse sendt til godkjenning, inneholder ${antallVarsler(packet)} varsler.")
            }
            if (erForlengelse(packet) && antallVarsler(packet) == 0) {
                counter.inc()
            }
        }

        private fun erForlengelse(packet: JsonMessage) = packet["aktivitetslogg.aktiviteter"]
                .takeIf(JsonNode::isArray)
                ?.filter { it["alvorlighetsgrad"].asText() == "INFO" }
                ?.any { it["melding"].asText().startsWith("Perioden er en forlengelse") } ?: false

        private fun antallVarsler(packet: JsonMessage) = packet["aktivitetslogg.aktiviteter"]
                .takeIf(JsonNode::isArray)
                ?.filter { it["alvorlighetsgrad"].asText() == "WARN" }
                ?.count() ?: 0
    }
}
