package no.nav.helse.spetakkel

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import io.prometheus.client.Summary
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory
import java.time.DayOfWeek

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
            validate { it.requireArray("utbetalingslinjer") {
                requireKey("dagsats")
                require("fom", JsonNode::asLocalDate)
                require("tom", JsonNode::asLocalDate)
            } }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        sikkerLogg.error("forstod ikke utbetalt:\n${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        utbetaltCounter.inc()
        val totalbeløp = packet["utbetalingslinjer"]
            .map {
                it["fom"].asLocalDate().datesUntil(it["tom"].asLocalDate().plusDays(1)).filter {
                    it.dayOfWeek !in listOf(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY)
                }.count().toInt() to it["dagsats"].asInt()
            }.sumBy { it.first * it.second }
        utbetalingBeløp.observe(totalbeløp.toDouble())
    }

}
