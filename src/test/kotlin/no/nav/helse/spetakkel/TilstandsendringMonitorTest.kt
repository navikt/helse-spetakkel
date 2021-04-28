package no.nav.helse.spetakkel

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*

class TilstandsendringMonitorTest {
    private val rapid = TestRapid()
    private val logCollector = ListAppender<ILoggingEvent>()
    private val dataSource = setupDataSourceMedFlyway()

    init {
        TilstandsendringMonitor(rapid, TilstandsendringMonitor.VedtaksperiodeTilstandDao(dataSource))
        (LoggerFactory.getLogger(TilstandsendringMonitor::class.java) as Logger).addAppender(logCollector)
        logCollector.start()
    }

    @BeforeEach
    fun setUp() {
        logCollector.list.clear()
    }

    @Test
    fun `Sender ikke varsel ved mindre enn 4 loops`() {
        val vedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP"
        ))

        Assertions.assertEquals(0, logCollector.list.count { it.message.contains("går i loop mellom") })
    }

    @Test
    fun `Sender varsel ved mer enn eller lik 4 loops`() {
        val vedtaksperiodeId = UUID.randomUUID()
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP"
        ))
        rapid.sendTestMessage(vedtaksperiodeEndret(
            vedtaksperiodeId = vedtaksperiodeId,
            forrigeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP",
            gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP"
        ))

        Assertions.assertEquals(1, logCollector.list.count { it.formattedMessage.contains("går i loop mellom AVVENTER_INNTEKTSMELDING_UFERDIG_GAP og AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP") })
        Assertions.assertEquals(1, logCollector.list.count { it.formattedMessage.contains("går i loop mellom AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP og AVVENTER_INNTEKTSMELDING_UFERDIG_GAP") })
    }

    @Language("JSON")
    private fun vedtaksperiodeEndret(
        eventId: UUID = UUID.randomUUID(),
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        aktørId: String = "10000123467",
        fødselsnummer: String = "20046913371",
        orgnummer: String = "98765432",
        forrigeTilstand: String = "MOTTATT_SYKMELDING_FERDIG_GAP",
        gjeldendeTilstand: String = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
    ) = """
{
  "vedtaksperiodeId": "$vedtaksperiodeId",
  "organisasjonsnummer": "$orgnummer",
  "gjeldendeTilstand": "$gjeldendeTilstand",
  "forrigeTilstand": "$forrigeTilstand",
  "aktivitetslogg": {
    "aktiviteter": []
  },
  "@event_name": "vedtaksperiode_endret",
  "@id": "$eventId",
  "@opprettet": "${LocalDateTime.now()}",
  "@forårsaket_av": {
    "event_name": "ny_søknad"
  },
  "makstid": "${LocalDateTime.MAX}",
  "aktørId": "$aktørId",
  "fødselsnummer": "$fødselsnummer"
}
"""
}
