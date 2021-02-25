package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

class ForlengelserUtenAdvarslerMonitor(
        rapidsConnection: RapidsConnection
) {

    private companion object {
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
                it.requireKey("vedtaksperiode_aktivitetslogg.aktiviteter")
            }
        }.register(TilGodkjenning())
    }

    class TilGodkjenning : River.PacketListener {

        override fun onPacket(packet: JsonMessage, context: MessageContext) {
            val aktiviterer = packet["vedtaksperiode_aktivitetslogg.aktiviteter"]
                    .takeIf(JsonNode::isArray)
                    ?.groupBy { it["alvorlighetsgrad"].asText() }

            val harAdvarsler = aktiviterer?.get("WARN") != null
            val erForlengelse = aktiviterer?.get("INFO")
                    ?.any { it["melding"].asText().startsWith("Perioden er en forlengelse") } == true

            if (erForlengelse && !harAdvarsler) {
                counter.inc()
            }
        }
    }
}
