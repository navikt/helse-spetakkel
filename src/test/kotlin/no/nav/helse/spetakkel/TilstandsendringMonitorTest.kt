package no.nav.helse.spetakkel

import com.github.navikt.tbd_libs.test_support.TestDataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

class TilstandsendringMonitorTest {
    private lateinit var rapid: TestRapid
    private lateinit var dataSource: TestDataSource

    @BeforeEach
    fun setup() {
        rapid = TestRapid()
        dataSource = databaseContainer.nyTilkobling()
        TilstandsendringMonitor(rapid, TilstandsendringMonitor.VedtaksperiodeTilstandDao(dataSource.ds))
    }

    @AfterEach
    fun teardown() {
        databaseContainer.droppTilkobling(dataSource)
    }

    @Test
    fun `avstemming`() {
        rapid.sendTestMessage(avstemmingmelding())
        assertEquals(2, sessionOf(dataSource.ds).use {
            it.run(queryOf("SELECT COUNT(1) FROM vedtaksperiode_tilstand").map { it.long(1) }.asSingle)
        })
    }

    @Test
    fun `Sender ikke varsel ved mindre enn 10 loops`() {
        val vedtaksperiodeId = UUID.randomUUID()
        repeat(9) {
            rapid.sendTestMessage(
                vedtaksperiodeEndret(
                    vedtaksperiodeId = vedtaksperiodeId,
                    forrigeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
                    gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP"
                )
            )
            rapid.sendTestMessage(
                vedtaksperiodeEndret(
                    vedtaksperiodeId = vedtaksperiodeId,
                    forrigeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP",
                    gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP"
                )
            )
        }

        assertFalse(meldinger().any { it["@event_name"].asText() == "vedtaksperiode_i_loop" })
    }

    @Test
    fun `Sender varsel ved mer enn eller lik 10 loops`() {
        val vedtaksperiodeId = UUID.randomUUID()
        repeat(10) {
            rapid.sendTestMessage(
                vedtaksperiodeEndret(
                    vedtaksperiodeId = vedtaksperiodeId,
                    forrigeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP",
                    gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP"
                )
            )
            rapid.sendTestMessage(
                vedtaksperiodeEndret(
                    vedtaksperiodeId = vedtaksperiodeId,
                    forrigeTilstand = "AVVENTER_INNTEKTSMELDING_UFERDIG_GAP",
                    gjeldendeTilstand = "AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP"
                )
            )
        }

        assertEquals(2, meldinger().filter { it["@event_name"].asText() == "vedtaksperiode_i_loop" }.size);
        val loopMelding = meldinger().first { it["@event_name"].asText() == "vedtaksperiode_i_loop" }
        assertEquals("AVVENTER_INNTEKTSMELDING_ELLER_HISTORIKK_FERDIG_GAP", loopMelding["forrigeTilstand"].asText());
        assertEquals("AVVENTER_INNTEKTSMELDING_UFERDIG_GAP", loopMelding["gjeldendeTilstand"].asText());
        assertEquals(vedtaksperiodeId.toString(), loopMelding["vedtaksperiodeId"].asText());
        assertEquals("vedtaksperiode_i_loop", loopMelding["@event_name"].asText());
    }

    @Test
    fun `revurdering og overstyring nullstiller loop-telleren`() {
        val vedtaksperiodeId = UUID.randomUUID()
        fun sendTilstandsendringer(startsted: String, vararg tilstander: String) {
            tilstander.forEachIndexed { index, tilstand ->
                if (index == 0) {
                    rapid.sendTestMessage(
                        vedtaksperiodeEndret(
                            vedtaksperiodeId = vedtaksperiodeId,
                            forrigeTilstand = startsted,
                            gjeldendeTilstand = tilstand
                        )
                    )
                } else if (index != tilstander.size - 1) {
                    rapid.sendTestMessage(
                        vedtaksperiodeEndret(
                            vedtaksperiodeId = vedtaksperiodeId,
                            forrigeTilstand = tilstand,
                            gjeldendeTilstand = tilstander[index+1]
                        )
                    )
                }
            }
        }

        sendTilstandsendringer(
            "Avsluttet",

            "AVVENTER_ARBEIDSGIVERE_REVURDERING",
            "AVVENTER_HISTORIKK_REVURDERING",
            "AVVENTER_GJENNOMFØRT_REVURDERING",

            "AVVENTER_ARBEIDSGIVERE_REVURDERING",
            "AVVENTER_HISTORIKK_REVURDERING",
            "AVVENTER_GJENNOMFØRT_REVURDERING",

            "AVVENTER_ARBEIDSGIVERE_REVURDERING",
            "AVVENTER_HISTORIKK_REVURDERING",
            "AVVENTER_GJENNOMFØRT_REVURDERING",

            "AVVENTER_ARBEIDSGIVERE_REVURDERING",
            "AVVENTER_HISTORIKK_REVURDERING",
            "AVVENTER_GJENNOMFØRT_REVURDERING",
        )

        assertEquals(0, meldinger().filter { it["@event_name"].asText() == "vedtaksperiode_i_loop" }.size);
    }

    private fun meldinger() = (0 until rapid.inspektør.size).map { rapid.inspektør.message(it) }


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

    @Language("JSON")
    private fun avstemmingmelding() = """
{
  "@event_name": "person_avstemt",
  "@id": "${UUID.randomUUID()}",
  "@opprettet": "${LocalDateTime.now()}",
  "aktørId": "11",
  "fødselsnummer": "22",
  "arbeidsgivere": [
    {
      "organisasjonsnummer": "987654321",
      "vedtaksperioder": [
        {
          "id": "${UUID.randomUUID()}",
          "tilstand": "AVVENTER_HISTORIKK",
          "opprettet": "${LocalDateTime.now()}",
          "oppdatert": "${LocalDateTime.now()}"
        }
      ],
      "forkastedeVedtaksperioder": [
        {
          "id": "${UUID.randomUUID()}",
          "tilstand": "TIL_INFOTRYGD",
          "opprettet": "${LocalDateTime.now()}",
          "oppdatert": "${LocalDateTime.now()}"
        }
      ]
    }
  ]
}
"""
}
