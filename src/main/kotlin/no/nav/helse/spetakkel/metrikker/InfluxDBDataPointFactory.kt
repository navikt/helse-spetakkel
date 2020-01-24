package no.nav.helse.spetakkel.metrikker

class InfluxDBDataPointFactory(private val defaultTags: Map<String, String> = emptyMap()) {

    fun createDataPoint(
            name: String,
            fields: Map<String, Any>,
            tags: Map<String, String>,
            timeInMilliseconds: Long = System.currentTimeMillis()
    ): InfluxDBDataPoint {
        return InfluxDBDataPoint(
                name = name,
                fields = fields,
                tags = tags + defaultTags,
                timeInMilliseconds = timeInMilliseconds
        )
    }
}
