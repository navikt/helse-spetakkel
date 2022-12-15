package no.nav.helse.spetakkel

import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class RevurderingIgangsattMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val revurderingerIgangsatt = Counter.build("revurdering_igangsatt", "Antall revurderinger igangsatt")
            .labelNames("hvorfor")
            .register()
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "revurdering_igangsatt")
                it.demandValue("typeEndring", "REVURDERING")
                it.requireKey("årsak")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        revurderingerIgangsatt.labels(packet["årsak"].asText()).inc()
    }
}
