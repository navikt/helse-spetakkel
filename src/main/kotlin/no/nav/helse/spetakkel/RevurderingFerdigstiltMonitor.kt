package no.nav.helse.spetakkel

import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class RevurderingFerdigstiltMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val revurderingerFerdigstilt = Counter.build("revurdering_ferdigstilt", "Antall revurderinger ferdigstilt")
            .labelNames("hvorfor", "status")
            .register()
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "revurdering_ferdigstilt")
                it.requireKey("status", "årsak")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        revurderingerFerdigstilt.labels(packet["årsak"].asText(), packet["status"].asText()).inc()
    }
}
