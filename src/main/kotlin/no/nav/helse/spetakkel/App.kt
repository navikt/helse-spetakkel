package no.nav.helse.spetakkel

import io.ktor.util.KtorExperimentalAPI
import no.nav.helse.rapids_rivers.RapidApplication

@KtorExperimentalAPI
fun main() {
    val env = System.getenv().toMutableMap()
    env.putIfAbsent("KAFKA_CONSUMER_GROUP_ID", "spetakkel-v1")
    env.putIfAbsent("KAFKA_RAPID_TOPIC", "helse-rapid-v1")

    RapidApplication.create(env).apply {
        TilstandsendringsRiver(this)
        PåminnelseMonitor(this)
    }.start()
}
