package no.nav.helse.spetakkel

import io.prometheus.client.Collector.MetricFamilySamples.Sample
import io.prometheus.client.CollectorRegistry
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*

internal class GodkjenningsbehovMonitorTest {

    private val rapid = TestRapid()
    private val dataSource = setupDataSourceMedFlyway()

    init {
        GodkjenningsbehovMonitor(rapid, dataSource)
    }

    @Test
    fun `registrerer godkjenningsbehov og løsning blir ikke plukket opp - version two`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehov()) }
        val godkjenningsbehovTotals = newSamples.single { it.name == "godkjenningsbehov_totals_total" }

        assertEquals(1.0, godkjenningsbehovTotals.value)
        assertEquals("OVERGANG_FRA_IT", godkjenningsbehovTotals.labelValue("periodetype"))
        assertEquals("FLERE_ARBEIDSGIVERE", godkjenningsbehovTotals.labelValue("inntektskilde"))
        assertEquals(0, newSamples.count { it.name == "godkjenningsbehovlosning_totals_total" })
    }

    @Test
    fun `registrerer godkjenningsbehovløsning og behov blir ikke plukket opp - version two`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehovløsning()) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehovlosning_totals_total" }

        assertEquals(1.0, godkjenningsbehovløsningTotals.value)
        assertEquals("FORLENGELSE", godkjenningsbehovløsningTotals.labelValue("periodetype"))
        assertEquals("EN_ARBEIDSGIVER", godkjenningsbehovløsningTotals.labelValue("inntektskilde"))
        assertEquals(0, newSamples.count { it.name == "godkjenningsbehov_totals_total" })
    }

    @Test
    fun `ta med utbetalingtype i løsning-metrikk`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehovløsning(utbetalingtype = "REVURDERING")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehovlosning_totals_total" }

        assertEquals("REVURDERING", godkjenningsbehovløsningTotals.labelValue("utbetalingtype"))
    }

    @Test
    fun `ta med utbetalingtype i behov-metrikk`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehov(utbetalingtype = "REVURDERING")) }
        val godkjenningsbehovTotals = newSamples.single { it.name == "godkjenningsbehov_totals_total" }

        assertEquals("REVURDERING", godkjenningsbehovTotals.labelValue("utbetalingtype"))
    }

    @Test
    fun `ta med forårsaket_av event_name i godkjenningsbehovmetric`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehov("behov")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehov_totals_total" }

        assertEquals("behov", godkjenningsbehovløsningTotals.labelValue("forarsaketAvEventName"))
    }

    @Test
    fun `ta med forårsaket_av event_name i godkjenningsbehovmetric 2`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehov("påminnelse")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehov_totals_total" }

        assertEquals("påminnelse", godkjenningsbehovløsningTotals.labelValue("forarsaketAvEventName"))
    }

    @Test
    fun `ta med forårsaket_av event_name i godkjenningsbehovløsningmetric`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehovløsning("behov")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehovlosning_totals_total" }

        assertEquals("behov", godkjenningsbehovløsningTotals.labelValue("forarsaketAvEventName"))
    }

    @Test
    fun `ta med forårsaket_av event_name i godkjenningsbehovløsningmetric 2`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehovløsning("påminnelse")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehovlosning_totals_total" }

        assertEquals("påminnelse", godkjenningsbehovløsningTotals.labelValue("forarsaketAvEventName"))
    }

    @Test
    fun `skiller mellom opprinnelig godkjenningsbehov og resending som følge av påminnelse`() {
        fun assertMarkertSomResending(tall: String, sample: List<Sample>) =
            assertEquals(
                tall,
                sample.single { it.name == "godkjenningsbehov_totals_total" }.labelValue("resendingAvGodkjenningsbehov")
            )


        val vedtaksperiodeId = UUID.randomUUID()

        val samples =
            forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehov(vedtaksperiodeId = vedtaksperiodeId)) }
        assertMarkertSomResending("0", samples)

        val samples2 =
            forskjellerIMetrikker {
                rapid.sendTestMessage(
                    godkjenningsbehov(
                        vedtaksperiodeId = vedtaksperiodeId,
                        forårsaketAvBehov = listOf("påminnelse")
                    )
                )
            }
        assertMarkertSomResending("1", samples2)
    }

    private fun Sample.labelValue(labelName: String): String {
        assertTrue (labelName in labelNames, "Kan ikke finne metrikk med label $labelName i $labelNames")
        return labelValues[labelNames.indexOf(labelName)]
    }

    private fun forskjellerIMetrikker(block: () -> Unit): List<Sample> {
        val before = CollectorRegistry.defaultRegistry.metricFamilySamples().toList().flatMap { it.samples }
        block()
        val after = CollectorRegistry.defaultRegistry.metricFamilySamples().toList().flatMap { it.samples }

        return after.map { valueAfter ->
            val valueBefore = before.find { it.name == valueAfter.name && it.labelValues == valueAfter.labelValues }
                ?: return@map valueAfter
            Sample(
                valueAfter.name,
                valueAfter.labelNames,
                valueAfter.labelValues,
                valueAfter.value - valueBefore.value
            )
        }.filter { it.value != 0.0 }
    }

    @Language("JSON")
    private fun godkjenningsbehov(
        forårsaketAvEventName: String = "behov",
        utbetalingtype: String = "UTBETALING",
        vedtaksperiodeId: UUID = UUID.randomUUID(),
        forårsaketAvBehov: List<String> = listOf("Simulering")
    ) = """{
  "@event_name": "behov",
  "@behovId": "1cb81cd8-a970-4868-bae7-f68572e62d0c",
  "@behov": [
    "Godkjenning"
  ],
  "meldingsreferanseId": "1dad39b0-f0d9-495b-a110-357d2397d7e0",
  "aktørId": "2902991512385",
  "fødselsnummer": "10047502468",
  "organisasjonsnummer": "947064649",
  "vedtaksperiodeId": "$vedtaksperiodeId",
  "tilstand": "AVVENTER_GODKJENNING",
  "utbetalingId": "0b8a2bdc-7e45-4a5e-abb6-99a79425b1db",
  "Godkjenning": {
    "periodeFom": "2022-01-01",
    "periodeTom": "2022-01-31",
    "skjæringstidspunkt": "2022-01-01",
    "periodetype": "OVERGANG_FRA_IT",
    "utbetalingtype": "$utbetalingtype",
    "inntektskilde": "FLERE_ARBEIDSGIVERE",
    "warnings": {
      "aktiviteter": [],
      "kontekster": []
    },
    "aktiveVedtaksperioder": [
      {
        "orgnummer": "947064649",
        "vedtaksperiodeId": "$vedtaksperiodeId",
        "periodetype": "OVERGANG_FRA_IT"
      }
    ],
    "orgnummereMedRelevanteArbeidsforhold": [
      "947064649"
    ],
    "arbeidsforholdId": ""
  },
  "@id": "a24f4f8f-a79f-41c5-9e2a-dd2d757b7146",
  "@opprettet": "2022-03-30T20:07:46.921797903",
  "@forårsaket_av": {
    "id": "38ba8528-2d61-473d-a803-06a3da6ad444",
    "opprettet": "2022-03-30T20:07:46.864972598",
    "event_name": "$forårsaketAvEventName",
    "behov": [${forårsaketAvBehov.joinToString { """"$it"""" }}]
  }
}
    """

    @Language("JSON")
    private fun godkjenningsbehovløsning(forårsaketAvEventName: String = "behov", utbetalingtype: String = "UTBETALING") = """{
  "@event_name": "behov",
  "@behovId": "1cb81cd8-a970-4868-bae7-f68572e62d0c",
  "@behov": [ "Godkjenning" ],
  "meldingsreferanseId": "1dad39b0-f0d9-495b-a110-357d2397d7e0",
  "aktørId": "2902991512385",
  "fødselsnummer": "10047502468",
  "organisasjonsnummer": "947064649",
  "vedtaksperiodeId": "91784677-a03d-4577-be0c-48b556aec0b4",
  "tilstand": "AVVENTER_GODKJENNING",
  "utbetalingId": "0b8a2bdc-7e45-4a5e-abb6-99a79425b1db",
  "Godkjenning": {
    "periodeFom": "2022-01-01",
    "periodeTom": "2022-01-31",
    "skjæringstidspunkt": "2022-01-01",
    "periodetype": "FORLENGELSE",
    "utbetalingtype": "$utbetalingtype",
    "inntektskilde": "EN_ARBEIDSGIVER",
    "warnings": {
      "aktiviteter": [],
      "kontekster": []
    },
    "aktiveVedtaksperioder": [
      {
        "orgnummer": "947064649",
        "vedtaksperiodeId": "91784677-a03d-4577-be0c-48b556aec0b4",
        "periodetype": "FORLENGELSE"
      }
    ],
    "orgnummereMedRelevanteArbeidsforhold": [ "947064649" ],
    "arbeidsforholdId": ""
  },
  "@id": "a24f4f8f-a79f-41c5-9e2a-dd2d757b7146",
  "@opprettet": "2022-03-30T20:07:46.921797903",
  "@forårsaket_av": {
    "id": "38ba8528-2d61-473d-a803-06a3da6ad444",
    "opprettet": "2022-03-30T20:07:46.864972598",
    "event_name": "$forårsaketAvEventName",
    "behov": [ "Godkjenning" ]
  },
  "@løsning": {
    "Godkjenning": {
      "godkjent": true,
      "saksbehandlerIdent": "Z123456",
      "saksbehandlerEpost": "saksbehandler@nav.no",
      "godkjenttidspunkt": "2022-03-30T20:07:46.825242074",
      "automatiskBehandling": true,
      "årsak": null,
      "begrunnelser": null,
      "kommentar": null
    }
  },
  "@final": true,
  "@besvart": "2022-03-30T20:07:46.921709323"
}
    """
}
