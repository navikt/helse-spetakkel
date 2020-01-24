package no.nav.helse.spetakkel

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory

class TilstandsendringsRiver() : Rapid.MessageListener {
    private val log = LoggerFactory.getLogger(TilstandsendringsRiver::class.java)

    private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())

    override fun onMessage(message: String) {
        val json = try {
            objectMapper.readTree(message)
        } catch (err: JsonProcessingException) {
            return
        }

        if (json.path("@event_name").asText() != "vedtaksperiode_endret") return

        log.info("håndterer vedtaksperiode_endret")
    }
}
