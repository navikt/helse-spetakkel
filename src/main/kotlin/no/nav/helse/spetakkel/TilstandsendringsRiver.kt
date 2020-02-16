package no.nav.helse.spetakkel

import no.nav.helse.rapids_rivers.*
import no.nav.helse.spetakkel.metrikker.InfluxDBDataPointFactory
import no.nav.helse.spetakkel.metrikker.SensuMetricReporter
import org.slf4j.LoggerFactory
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
            validate { it.requireValue("@event_name", "vedtaksperiode_endret") }
            validate { it.requireKey("vedtaksperiodeId") }
            validate { it.requireKey("gjeldendeTilstand") }
            validate { it.requireKey("forrigeTilstand") }
            validate { it.requireKey("endringstidspunkt") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
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

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
    }

}
