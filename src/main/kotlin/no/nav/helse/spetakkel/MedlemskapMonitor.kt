package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory

internal class MedlemskapMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(MedlemskapMonitor::class.java)
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

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        packet["@løsning.Medlemskap.resultat.svar"].asText()
            .also {
                Counter.builder("medlemskapvurdering_totals")
                    .description("Antall medlemskapvurderinger")
                    .tag("resultat", it)
                    .register(meterRegistry)
                    .increment()
            }

        sjekkDelresultat(meterRegistry, packet["@løsning.Medlemskap.resultat"])
    }

    private fun sjekkDelresultat(meterRegistry: MeterRegistry, node: JsonNode) {
        if (node.path("delresultat").let { it.isArray && !it.isEmpty }) {
            node.path("delresultat").map { sjekkDelresultat(meterRegistry, it) }
            return
        }

        Counter.builder("medlemskapresultat_totals")
            .description("Antall medlemskapvurderinger")
            .tag("identifikator", node.path("identifikator").asText())
            .tag("svar", node.path("svar").asText())
            .register(meterRegistry)
            .increment()
    }
}
