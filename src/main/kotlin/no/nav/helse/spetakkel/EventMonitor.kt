package no.nav.helse.spetakkel

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry

class EventMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply { validate { it.requireKey("@event_name") } }
            .register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        Counter.builder("hendelser_totals")
            .description("Antall hendelser")
            .tags("hendelse", packet["@event_name"].asText())
            .tag("topic", metadata.topic)
            .tag("partition", metadata.partition.toString())
            .register(meterRegistry)
            .increment()
    }
}
