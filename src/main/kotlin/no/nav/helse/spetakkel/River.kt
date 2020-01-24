package no.nav.helse.spetakkel

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

abstract class River: Rapid.MessageListener {

    private val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())

    private val validations = mutableListOf<(JsonNode) -> Boolean>()

    fun validate(validation: (JsonNode) -> Boolean) {
        validations.add(validation)
    }

    override fun onMessage(message: String) {
        val packet = try {
            objectMapper.readTree(message)
        } catch (err: JsonProcessingException) {
            return
        }

        for (v in validations) if (!v(packet)) return

        onPacket(packet)
    }

    abstract fun onPacket(packet: JsonNode)
}
