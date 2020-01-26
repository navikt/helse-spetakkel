package no.nav.helse.spetakkel

import no.nav.helse.rapids_rivers.AppBuilder

fun main() {
    val env = System.getenv().toMutableMap()
    env.putIfAbsent("KAFKA_CONSUMER_GROUP_ID", "spetakkel-v1")
    env.putIfAbsent("KAFKA_RAPID_TOPIC", "privat-helse-sykepenger-rapid-v1")

    val appBuilder = AppBuilder(env)
    appBuilder.register(TilstandsendringsRiver())
    appBuilder.start()
}
