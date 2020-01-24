package no.nav.helse.spetakkel.metrikker

import java.util.concurrent.TimeUnit

class InfluxDBDataPoint(
        private val name: String,
        private val fields: Map<String, Any>,
        private val tags: Map<String, String> = emptyMap(),
        val timeInMilliseconds: Long = System.currentTimeMillis()
) {

    init {
        require(fields.isNotEmpty())
    }

    fun toLineProtocol(): String {
        return String.format("%s%s%s %d", escapeMeasurement(name), if (tags.isNotEmpty()) "," + tags.toCSV() else "", if (fields.isNotEmpty()) " " + transformFields(fields) else "", TimeUnit.MILLISECONDS.toNanos(timeInMilliseconds))
    }

    private fun transformFields(fields: Map<String, Any>) =
            fields.map { entry ->
                "${escapeTagKeysAndValues(entry.key)}=" + when (entry.value) {
                    is String -> "\"${escapeFieldValue(entry.value as String)}\""
                    is Boolean -> entry.value
                    is Int -> "${entry.value}i"
                    else -> entry.value
                }
            }.joinToString(separator = ",")

    private fun Map<String, String>.toCSV() =
            map { entry ->
                "${escapeTagKeysAndValues(entry.key)}=${escapeTagKeysAndValues(entry.value)}"
            }.joinToString(separator = ",")

    private fun escapeTagKeysAndValues(str: String) =
            str.replace("=", "\\=")
                    .replace(",", "\\,")
                    .replace(" ", "\\ ")

    private fun escapeMeasurement(str: String) =
            str.replace(",", "\\,")
                    .replace(" ", "\\ ")

    private fun escapeFieldValue(str: String) =
            str.replace("\"", "\\\"")
}

