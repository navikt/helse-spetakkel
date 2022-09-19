package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.*
import no.nav.helse.spetakkel.AktivitetsloggMonitor.Nivå.FUNKSJONELL_FEIL
import no.nav.helse.spetakkel.AktivitetsloggMonitor.Nivå.VARSEL
import org.slf4j.LoggerFactory
import java.util.*

internal class AktivitetsloggMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(AktivitetsloggMonitor::class.java)

        private val aktivitetCounter = Counter.build("aktivitet_totals", "Antall aktiviteter")
            .labelNames("alvorlighetsgrad", "melding")
            .register()
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.requireValue("@event_name", "aktivitetslogg_ny_aktivitet")
                it.requireArray("aktiviteter") {
                    requireAny("nivå", Nivå.values().map { nivå -> nivå.toString() })
                    requireKey("melding")
                }
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        packet["aktiviteter"]
            .takeIf(JsonNode::isArray)
            ?.map {
                Nivå.valueOf(it.path("nivå").asText()) to it
            }
            ?.filter { (nivå, _) -> nivå in listOf(VARSEL, FUNKSJONELL_FEIL) }
            ?.onEach { (nivå, aktivitet) ->
                aktivitetCounter.labels(nivå.name, aktivitet["melding"].asText()).inc()
            }
    }

    enum class Nivå {
        INFO,
        VARSEL,
        FUNKSJONELL_FEIL,
        LOGISK_FEIL;
    }
}
