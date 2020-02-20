package no.nav.helse.spetakkel

import io.ktor.util.KtorExperimentalAPI
import no.nav.helse.rapids_rivers.RapidApplication
import kotlin.time.ExperimentalTime

@ExperimentalTime
@KtorExperimentalAPI
fun main() {
    val env = System.getenv().toMutableMap()
    env.putIfAbsent("KAFKA_CONSUMER_GROUP_ID", "spetakkel-v1")
    env.putIfAbsent("KAFKA_RAPID_TOPIC", "helse-rapid-v1")

    val dataSourceBuilder = DataSourceBuilder(env)
    dataSourceBuilder.migrate()

    val slackClient = env["SLACK_WEBHOOK_URL"]?.let {
        SlackClient(
            webhookUrl = it,
            defaultChannel = "#team-bømlo-alerts",
            defaultUsername = "spetakkel"
        )
    }

    RapidApplication.create(env).apply {
        TilstandsendringsRiver(this)
        PåminnelseMonitor(this, slackClient)
        TilstandsendringMonitor(this, TilstandsendringMonitor.VedtaksperiodeTilstandDao(dataSourceBuilder.getDataSource()))
        TidITilstandMonitor(this, slackClient)
        VedtaksperiodePåminnetMonitor(this)
    }.start()
}
