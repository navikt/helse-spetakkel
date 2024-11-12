package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.spetakkel.AktivitetsloggMonitor.Nivå.FUNKSJONELL_FEIL
import no.nav.helse.spetakkel.AktivitetsloggMonitor.Nivå.VARSEL
import org.slf4j.LoggerFactory

internal class AktivitetsloggMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(AktivitetsloggMonitor::class.java)
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "aktivitetslogg_ny_aktivitet")
                it.requireArray("aktiviteter") {
                    requireKey("nivå", "melding")
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        packet["aktiviteter"]
            .takeIf(JsonNode::isArray)
            ?.filter {
                it.path("nivå").asText() in Nivå.values().map(Enum<*>::name)
            }
            ?.map {
                Nivå.valueOf(it.path("nivå").asText()) to it
            }
            ?.filter { (nivå, _) -> nivå in listOf(VARSEL, FUNKSJONELL_FEIL) }
            ?.onEach { (nivå, aktivitet) ->
                Counter.builder("aktivitet_totals")
                    .description("Antall aktiviteter")
                    .tag("alvorlighetsgrad", nivå.name)
                    .tag("melding", aktivitet.path("melding").asText())
                    .register(meterRegistry)
                    .increment()
            }
    }

    enum class Nivå {
        INFO,
        BEHOV,
        VARSEL,
        FUNKSJONELL_FEIL,
        LOGISK_FEIL;
    }
}
