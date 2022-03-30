package no.nav.helse.spetakkel

import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory

internal class Infotrygdendringer(rapidsConnection: RapidsConnection) : River.PacketListener {
    private companion object {
        private val counter = Counter.build("infotrygdendringer", "Antall infotrygdendringer")
            .register()
    }

    init {
        River(rapidsConnection).apply {
            validate { it.demandValue("@event_name", "infotrygdendting") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        counter.inc()
    }
}
