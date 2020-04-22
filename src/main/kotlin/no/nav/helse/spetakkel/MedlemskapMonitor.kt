package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory

internal class MedlemskapMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(MedlemskapMonitor::class.java)

        private val medlemskapvurderingCounter = Counter.build("medlemskapvurdering_totals", "Antall medlemskapvurderinger")
            .labelNames("resultat")
            .register()
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "behov")
                it.demandAll("@behov", listOf("Medlemskap"))
                it.demandKey("@løsning.Medlemskap")
                it.interestedIn("@løsning.Medlemskap.resultat.svar")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        packet["@løsning.Medlemskap.resultat.svar"]
            .takeIf(JsonNode::isTextual)
            ?.asText()
            ?.also { medlemskapvurderingCounter.labels(it).inc() } ?: medlemskapvurderingCounter.labels("FEIL_I_OPPSLAG").inc()
    }
}
