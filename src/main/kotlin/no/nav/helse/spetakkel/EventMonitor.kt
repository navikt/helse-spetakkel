package no.nav.helse.spetakkel

import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

class EventMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val eventCounter =
            Counter.build("hendelser_totals", "Antall hendelser")
                .labelNames("hendelse")
                .register()
    }

    init {
        River(rapidsConnection)
            .apply { validate { it.requireKey("@event_name") } }
            .register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        eventCounter.labels(packet["@event_name"].asText()).inc()
    }
}
