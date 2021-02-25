package no.nav.helse.spetakkel

import io.prometheus.client.Counter
import io.prometheus.client.Summary
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class UtbetaltMonitor(rapidsConnection: RapidsConnection) : River.PacketListener {
    private companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val utbetaltCounter =
            Counter.build("utbetaling_totals", "Antall utbetalinger")
                .register()
        private val utbetalingBeløp: Summary = Summary.build()
            .name("utbetaling_totalbelop_kroner")
            .quantile(0.5, 0.05) // Add 50th percentile (= median) with 5% tolerated error
            .quantile(0.9, 0.01) // Add 90th percentile with 1% tolerated error
            .quantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
            .help("Totalbeløp for et oppdrag i kroner")
            .register()
    }

    init {
        River(rapidsConnection).apply {
            validate { it.demandValue("@event_name", "utbetalt") }
            validate { it.requireArray("utbetalt") {
                requireKey("totalbeløp")
            } }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        sikkerLogg.error("forstod ikke utbetalt:\n${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        utbetaltCounter.inc()
        val totalbeløp = packet["utbetalt"]
            .map { it["totalbeløp"].asInt() }
            .sum()
        utbetalingBeløp.observe(totalbeløp.toDouble())
    }

}
