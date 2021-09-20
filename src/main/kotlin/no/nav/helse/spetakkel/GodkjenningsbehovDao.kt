package no.nav.helse.spetakkel

import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.util.*
import javax.sql.DataSource

class GodkjenningsbehovDao(private val dataSource: DataSource) {

    fun erOpprinneligGodkjenningsbehov(id: UUID): Boolean {
        sessionOf(dataSource).use {
            @Language("PostgreSQL")
            val query = "INSERT INTO vedtaksperiode_godkjenningsbehov_duplikatsjekk (vedtaksperiode_id) VALUES (?) ON CONFLICT DO NOTHING;"
            return it.run(
                queryOf(query, id).asUpdate
            ) > 0
        }
    }
}