package no.nav.helse.spetakkel

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import no.nav.helse.rapids_rivers.RapidApplication

fun main() {
    val env = System.getenv()
    val dataSourceBuilder = DataSourceBuilder(env)

    val dataSource = dataSourceBuilder.getDataSource()

    RapidApplication.create(env).apply {
        EventMonitor(this)
        AktivitetsloggMonitor(this)
        RevurderingIgangsattMonitor(this)
        RevurderingFerdigstiltMonitor(this)
        TelleverkRiver(this, OppfriskTilstandstellingDao(dataSource))
        TilstandsendringMonitor(this, TilstandsendringMonitor.VedtaksperiodeTilstandDao(dataSource))
        VedtaksperiodePåminnetMonitor(this)
        BehovMonitor(this)
        GodkjenningsbehovMonitor(this, dataSource)
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
