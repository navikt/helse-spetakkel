package no.nav.helse.spetakkel

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry

internal class RevurderingFerdigstiltMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "revurdering_ferdigstilt")
                it.requireKey("status", "årsak")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        Counter.builder("revurdering_ferdigstilt")
            .description("Antall revurderinger ferdigstilt")
            .tag("hvorfor", packet["årsak"].asText())
            .tag("status", packet["status"].asText())
            .register(meterRegistry)
    }
}
