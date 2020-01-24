package no.nav.helse.spetakkel

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory

class TilstandsendringsRiver() : Rapid.MessageListener {
    private val log = LoggerFactory.getLogger(TilstandsendringsRiver::class.java)

    private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())

    override fun onMessage(message: String) {
        log.info("leser melding")
    }
}
