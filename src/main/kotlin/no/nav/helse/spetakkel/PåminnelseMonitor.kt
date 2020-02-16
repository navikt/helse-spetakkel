package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class PåminnelseMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(PåminnelseMonitor::class.java)
        private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "påminnelse") }
            validate { it.requireKey("vedtaksperiodeId") }
            validate { it.requireKey("antallGangerPåminnet") }
            validate { it.requireKey("endringstidspunkt") }
            validate { it.requireKey("tilstand") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        if (2 > packet["antallGangerPåminnet"].asInt()) return
        alert(packet)
    }

    private fun alert(packet: JsonMessage) {
        log.error(
            "vedtaksperiode {} sitter fast i tilstand {}; har blitt påminnet {} ganger siden {}",
            keyValue("vedtaksperiodeId", packet["vedtaksperiode"].asText()),
            keyValue("tilstand", packet["tilstand"].asText()),
            keyValue("antallGangerPåminnet", packet["antallGangerPåminnet"].asInt()),
            keyValue("endringstidspunkt", packet["endringstidspunkt"].asLocalDateTime())
        )
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {}
}
