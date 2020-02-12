package no.nav.helse.spetakkel

import no.nav.helse.rapids_rivers.RapidApplication

fun main() {
    val env = System.getenv().toMutableMap()
    env.putIfAbsent("KAFKA_CONSUMER_GROUP_ID", "spetakkel-v1")
    env.putIfAbsent("KAFKA_RAPID_TOPIC", "helse-rapid-v1")

    RapidApplication.Companion.Builder(env)
            .build()
            .apply { TilstandsendringsRiver(this) }
            .start()
}
