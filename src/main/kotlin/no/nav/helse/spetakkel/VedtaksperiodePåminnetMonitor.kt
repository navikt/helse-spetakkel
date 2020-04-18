package no.nav.helse.spetakkel

import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class VedtaksperiodePåminnetMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val vedtaksperiodePåminnetCounter =
            Counter.build("vedtaksperiode_paminnet_totals", "Antall ganger en vedtaksperiode er blitt påminnet")
                .labelNames("tilstand")
                .register()
    }

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "vedtaksperiode_påminnet") }
            validate { it.requireKey("tilstand") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        vedtaksperiodePåminnetCounter.labels(packet["tilstand"].asText()).inc()
    }
}
