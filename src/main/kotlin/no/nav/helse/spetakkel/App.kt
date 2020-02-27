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

    val slackClient = env["SLACK_ACCESS_TOKEN"]?.let {
        SlackClient(
            accessToken = it,
            channel = env.getValue("SLACK_CHANNEL_ID")
        )
    }

    val dataSource = dataSourceBuilder.getDataSource()
    val slackThreadDao = SlackThreadDao(dataSource)

    RapidApplication.create(env).apply {
        TilstandsendringsRiver(this)
        EventMonitor(this)
        PåminnelseMonitor(this, slackClient, slackThreadDao)
        TilstandsendringMonitor(this, TilstandsendringMonitor.VedtaksperiodeTilstandDao(dataSource))
        TidITilstandMonitor(this, slackClient, slackThreadDao)
        VedtaksperiodePåminnetMonitor(this)
        BehovMonitor(this)
        UtbetalingMonitor(this, slackClient)
    }.start()
}
