package no.nav.helse.spetakkel
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import javax.sql.DataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using

internal class TelleverkRiver(
    rapidsConnection: RapidsConnection,
    val dao: OppfriskTilstandstellingDao
) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply {
                precondition { it.requireAny("@event_name", listOf("kvarter", "spetakkel_oppdater_telleverk")) }
            }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        dao.friskOppTilstandstelling()
    }

    override fun onSevere(error: MessageProblems.MessageException, context: MessageContext) {
        println("lots of severe")
    }
}

internal class OppfriskTilstandstellingDao(val dataSource: DataSource) {
    fun friskOppTilstandstelling() {
        using(sessionOf(dataSource)) { session ->
            session.run(queryOf("REFRESH MATERIALIZED VIEW tilstandstelling").asExecute)}
    }
}
