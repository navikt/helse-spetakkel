package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import org.slf4j.LoggerFactory

class TilstandsendringsRiver() : River() {
    private val log = LoggerFactory.getLogger(TilstandsendringsRiver::class.java)

    init {
        validate { it.path("@event_name").asText() == "vedtaksperiode_endret" }
    }

    override fun onPacket(packet: JsonNode) {
        log.info("håndterer vedtaksperiode_endret")
    }
}
