package no.nav.helse.spetakkel

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.testcontainers.containers.PostgreSQLContainer
import java.time.Duration
import javax.sql.DataSource


internal fun setupDataSourceMedFlyway(): DataSource {
    val postgres = PostgreSQLContainer<Nothing>("postgres:13").also { it.start() }
    val hikariConfig = HikariConfig().apply {
        jdbcUrl = postgres.jdbcUrl
        username = postgres.username
        password = postgres.password
        maximumPoolSize = 2
        connectionTimeout = Duration.ofSeconds(5).toMillis()
        maxLifetime = Duration.ofMinutes(30).toMillis()
        initializationFailTimeout = Duration.ofMinutes(1).toMillis()
    }

    val dataSource = HikariDataSource(hikariConfig)

    Flyway.configure()
        .dataSource(dataSource)
        .load()
        .migrate()

    return dataSource
}
