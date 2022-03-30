package no.nav.helse.spetakkel

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.time.Duration

internal class MigrationTest {

    private val postgres = PostgreSQLContainer<Nothing>("postgres:13")
    private lateinit var hikariConfig: HikariConfig

    @BeforeEach
    fun `start postgres`() {
        postgres.start()
        hikariConfig = HikariConfig().apply {
            jdbcUrl = postgres.jdbcUrl
            username = postgres.username
            password = postgres.password
            maximumPoolSize = 1
            connectionTimeout = Duration.ofSeconds(5).toMillis()
            maxLifetime = Duration.ofMinutes(30).toMillis()
            initializationFailTimeout = Duration.ofMinutes(1).toMillis()
        }
    }

    private fun runMigration() =
        Flyway.configure()
            .dataSource(HikariDataSource(hikariConfig))
            .load()
            .migrate()

    private fun createHikariConfig(jdbcUrl: String) =
        HikariConfig().apply {
            this.jdbcUrl = jdbcUrl
            maximumPoolSize = 1
            connectionTimeout = Duration.ofSeconds(5).toMillis()
            maxLifetime = Duration.ofMinutes(30).toMillis()
            initializationFailTimeout = Duration.ofMinutes(1).toMillis()
        }

    @AfterEach
    fun `stop postgres`() {
        postgres.stop()
    }

    @Test
    fun `migreringer skal kjøre`() {
        val migrations = runMigration()
        assertTrue(migrations.migrationsExecuted > 0, "Ingen migreringer ble kjørt")
    }
}
