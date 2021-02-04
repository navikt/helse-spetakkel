package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class BehovUtenLøsningMonitor(
    rapidsConnection: RapidsConnection
) : River.PacketListener {

    private companion object {
        private val sikkerLog = LoggerFactory.getLogger("tjenestekall")
    }

    private val behovUtenLøsningCounter =
        Counter.build("behov_uten_losning_totals", "Antall behov uten løsning")
            .labelNames("behovtype")
            .register()

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "behov_uten_fullstendig_løsning")
                it.requireKey("@id", "behov_id", "ufullstendig_behov")
                it.requireArray("forventet")
                it.requireArray("løsninger")
                it.requireArray("mangler")
                it.require("@opprettet", JsonNode::asLocalDateTime)
                it.require("behov_opprettet", JsonNode::asLocalDateTime) }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        sikkerLog.error("forstod ikke behov_uten_fullstendig_løsning:\n${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        behovUtenLøsningCounter
            .labels(packet["mangler"].joinToString())
            .inc()
    }
}
