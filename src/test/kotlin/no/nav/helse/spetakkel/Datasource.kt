package no.nav.helse.spetakkel

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import javax.sql.DataSource


internal fun setupDataSourceMedFlyway(): DataSource {
    val hikariConfig = HikariConfig().apply {
        this.jdbcUrl = EmbeddedPostgres.builder()
            .start()
            .getJdbcUrl("postgres", "postgres")
        maximumPoolSize = 3
        minimumIdle = 1
        idleTimeout = 10001
        connectionTimeout = 1000
        maxLifetime = 30001
    }

    val dataSource = HikariDataSource(hikariConfig)

    Flyway.configure()
        .dataSource(dataSource)
        .load()
        .migrate()

    return dataSource
}
