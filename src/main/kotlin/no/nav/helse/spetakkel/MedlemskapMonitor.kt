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

        private val medlemskapresultatCounter = Counter.build("medlemskapresultat_totals", "Antall medlemskapvurderinger")
            .labelNames("identifikator", "svar")
            .register()
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "behov")
                it.demandAll("@behov", listOf("Medlemskap"))
                it.demandKey("@løsning.Medlemskap.resultat")
                it.requireKey("@løsning.Medlemskap.resultat.svar",
                    "@løsning.Medlemskap.resultat.delresultat")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        packet["@løsning.Medlemskap.resultat.svar"].asText()
            .also { medlemskapvurderingCounter.labels(it).inc() }

        sjekkDelresultat(packet["@løsning.Medlemskap.resultat"])
    }

    private fun sjekkDelresultat(node: JsonNode) {
        if (node.path("delresultat").let { it.isArray && !it.isEmpty }) {
            node.path("delresultat").map { sjekkDelresultat(it) }
            return
        }

        medlemskapresultatCounter.labels(node.path("identifikator").asText(), node.path("svar").asText()).inc()
    }
}
