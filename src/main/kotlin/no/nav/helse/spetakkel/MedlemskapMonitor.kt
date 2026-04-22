package no.nav.helse.spetakkel

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry

internal class MedlemskapMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            precondition {
                it.requireValue("@event_name", "behov")
                it.requireAll("@behov", listOf("Medlemskap"))
                it.requireKey("@løsning.Medlemskap.resultat")
            }
            validate {
                it.requireKey("@løsning.Medlemskap.resultat.svar")
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
    }
}
