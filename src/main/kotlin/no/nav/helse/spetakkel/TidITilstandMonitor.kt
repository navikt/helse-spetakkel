package no.nav.helse.spetakkel

import io.prometheus.client.Histogram
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory
import kotlin.time.ExperimentalTime
import kotlin.time.days
import kotlin.time.hours
import kotlin.time.minutes

@ExperimentalTime
internal class TidITilstandMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(TidITilstandMonitor::class.java)
        private val histogram = Histogram.build(
            "vedtaksperiode_tilstand_latency_seconds",
            "Antall sekunder en vedtaksperiode er i en tilstand"
        )
            .labelNames("tilstand")
            .buckets(
                1.minutes.inSeconds,
                1.hours.inSeconds,
                12.hours.inSeconds,
                24.hours.inSeconds,
                7.days.inSeconds,
                30.days.inSeconds
            )
            .register()
    }

    init {
        River(rapidsConnection).apply {
            validate { it.requireValue("@event_name", "vedtaksperiode_tid_i_tilstand") }
            validate { it.requireKey("tilstand") }
            validate { it.requireKey("tid_i_tilstand") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val tidITilstand = TidITilstand(packet)
        tidITilstand.observe(histogram)
    }

    private class TidITilstand(private val packet: JsonMessage) {
        val tilstand: String get() = packet["tilstand"].asText()
        val tidITilstand: Long get() = packet["tid_i_tilstand"].asLong()

        fun observe(histogram: Histogram) {
            histogram.labels(tilstand).observe(tidITilstand.toDouble())
        }
    }
}
