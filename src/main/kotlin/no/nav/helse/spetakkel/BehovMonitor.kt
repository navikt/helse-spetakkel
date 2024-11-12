package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.slf4j.LoggerFactory

internal class BehovMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(BehovMonitor::class.java)
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "behov")
                it.rejectKey("@løsning")
                it.requireKey("@behov")
                it.interestedIn("vedtaksperiodeId")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        packet["@behov"]
            .takeIf(JsonNode::isArray)
            ?.map(JsonNode::asText)
            ?.onEach { behov ->
                Counter.builder("behov_totals")
                    .description("Antall behov opprettet")
                    .tags("behovType", behov)
                    .register(meterRegistry)
                    .increment()
            }
            ?.also { behov ->
                packet["vedtaksperiodeId"].takeIf(JsonNode::isTextual)?.asText()?.also {
                    log.info("{} har behov for {}", keyValue("vedtaksperiodeId", it), behov)
                }
            }
    }
}
