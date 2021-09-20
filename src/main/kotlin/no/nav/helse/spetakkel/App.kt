package no.nav.helse.spetakkel

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import kotlin.time.ExperimentalTime

@ExperimentalTime
fun main() {
    val env = System.getenv()
    val dataSourceBuilder = DataSourceBuilder(env)

    val dataSource = dataSourceBuilder.getDataSource()

    RapidApplication.create(env).apply {
        EventMonitor(this)
        AktivitetsloggMonitor(this)
        TilstandsendringMonitor(this, TilstandsendringMonitor.VedtaksperiodeTilstandDao(dataSource))
        TidITilstandMonitor(this)
        VedtaksperiodePåminnetMonitor(this)
        BehovMonitor(this)
        GodkjenningsbehovMonitor(this, GodkjenningsbehovDao(dataSource))
        UtbetaltMonitor(this)
        MedlemskapMonitor(this)
        BehovUtenLøsningMonitor(this)
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}
