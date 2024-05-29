package no.nav.helse.spetakkel

import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers

val databaseContainer = DatabaseContainers.container("spetakkel", CleanupStrategy.tables("vedtaksperiode_endret_historikk, vedtaksperiode_godkjenningsbehov_duplikatsjekk, vedtaksperiode_tilstand"))

