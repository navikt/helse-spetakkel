package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory

internal class BehovMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(BehovMonitor::class.java)

        private val behovCounter = Counter.build("behov_totals", "Antall behov opprettet")
            .labelNames("behovType")
            .register()
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

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        packet["@behov"]
            .takeIf(JsonNode::isArray)
            ?.map(JsonNode::asText)
            ?.onEach { behov -> behovCounter.labels(behov).inc() }
            ?.also { behov ->
                packet["vedtaksperiodeId"].takeIf(JsonNode::isTextual)?.asText()?.also {
                    log.info("{} har behov for {}", keyValue("vedtaksperiodeId", it), behov)
                }
            }
    }
}
