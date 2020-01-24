package no.nav.helse.spetakkel.metrikker

class SensuMetricReporter(private val hostname: String,
                          private val port: Int,
                          private val sensuCheckName: String) {

    fun report(dataPoint: InfluxDBDataPoint): SensuEvent {
        return report(dataPoint.toLineProtocol())
    }

    fun report(dataPoint: String): SensuEvent {
        val event = SensuEvent(sensuCheckName, "metric", "events_nano", dataPoint)
        event.send(hostname, port)
        return event
    }
}
