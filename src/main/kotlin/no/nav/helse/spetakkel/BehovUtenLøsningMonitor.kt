package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory

internal class BehovUtenLøsningMonitor(
    rapidsConnection: RapidsConnection
) : River.PacketListener {

    private companion object {
        private val sikkerLog = LoggerFactory.getLogger("tjenestekall")
    }

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

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        sikkerLog.error("forstod ikke behov_uten_fullstendig_løsning:\n${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        Counter.builder("behov_uten_losning_totals")
            .description("Antall behov uten løsning")
            .tags("behovtype", packet["mangler"].joinToString())
            .register(meterRegistry)
            .increment()
    }
}
