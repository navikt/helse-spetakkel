package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.spetakkel.metrikker.InfluxDBDataPointFactory
import no.nav.helse.spetakkel.metrikker.SensuMetricReporter
import org.slf4j.LoggerFactory
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import java.time.LocalDateTime
import java.time.ZoneId


class TilstandsendringsRiver(rapidsConnection: RapidsConnection) : River.PacketListener {
    private val log = LoggerFactory.getLogger(TilstandsendringsRiver::class.java)

    private val dataPointFactory = InfluxDBDataPointFactory(mapOf(
            "application" to (System.getenv("NAIS_APP_NAME") ?: "spetakkel"),
            "cluster" to (System.getenv("NAIS_CLUSTER_NAME") ?: "dev-fss"),
            "namespace" to (System.getenv("NAIS_NAMESPACE") ?: "default")
    ))

    private val sensu = SensuMetricReporter("sensu.nais", 3030, "spetakkel-events")
    private val timezone = ZoneId.of("Europe/Oslo")

    init {
        River(rapidsConnection).apply {
            validate { it.path("@event_name").asText() == "vedtaksperiode_endret" }
            validate { it.hasNonNull("vedtaksperiodeId") }
            validate { it.hasNonNull("gjeldendeTilstand") }
            validate { it.hasNonNull("forrigeTilstand") }
            validate { it.hasNonNull("endringstidspunkt") }
        }.register(this)
    }

    override fun onPacket(packet: JsonNode, context: RapidsConnection.MessageContext) {
        val endringstidspunkt = packet["endringstidspunkt"]
                .asLocalDateTime()
                .atZone(timezone)

        sensu.report(dataPointFactory.createDataPoint("vedtaksperiode_endret.event", mapOf(
                "endringstidspunkt" to "$endringstidspunkt"
        ), mapOf(
                "vedtaksperiodeId" to packet["vedtaksperiodeId"].asText(),
                "gjeldendeTilstand" to packet["gjeldendeTilstand"].asText(),
                "forrigeTilstand" to packet["forrigeTilstand"].asText()
        ), endringstidspunkt.toInstant()
                .toEpochMilli()))
    }

    private fun JsonNode.asLocalDateTime() =
            asText().let { LocalDateTime.parse(it) }

}
