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

    val rapid = TestRapid()
    private val dataSource = setupDataSourceMedFlyway()

    init {
        GodkjenningsbehovMonitor(rapid, dataSource)
    }

    @Test
    fun `registrerer godkjenningsbehov og løsning blir ikke plukket opp - version two`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehov()) }
        val godkjenningsbehovTotals = newSamples.single { it.name == "godkjenningsbehov_totals" }

        assertEquals(1.0, godkjenningsbehovTotals.value)
        assertEquals("OVERGANG_FRA_IT", godkjenningsbehovTotals.labelValue("periodetype"))
        assertEquals("FLERE_ARBEIDSGIVERE", godkjenningsbehovTotals.labelValue("inntektskilde"))
        assertEquals(0, newSamples.count { it.name == "godkjenningsbehovlosning_totals" })
    }

    @Test
    fun `registrerer godkjenningsbehovløsning og behov blir ikke plukket opp - version two`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehovløsning()) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehovlosning_totals" }

        assertEquals(1.0, godkjenningsbehovløsningTotals.value)
        assertEquals("FORLENGELSE", godkjenningsbehovløsningTotals.labelValue("periodetype"))
        assertEquals("EN_ARBEIDSGIVER", godkjenningsbehovløsningTotals.labelValue("inntektskilde"))
        assertEquals(0, newSamples.count { it.name == "godkjenningsbehov_totals" })
    }

    @Test
    fun `ta med utbetalingtype i løsning-metrikk`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehovløsning(utbetalingtype = "REVURDERING")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehovlosning_totals" }

        assertEquals("REVURDERING", godkjenningsbehovløsningTotals.labelValue("utbetalingtype"))
    }

    @Test
    fun `ta med utbetalingtype i behov-metrikk`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehov(utbetalingtype = "REVURDERING")) }
        val godkjenningsbehovTotals = newSamples.single { it.name == "godkjenningsbehov_totals" }

        assertEquals("REVURDERING", godkjenningsbehovTotals.labelValue("utbetalingtype"))
    }

    @Test
    fun `ta med forårsaket_av event_name i godkjenningsbehovmetric`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehov("behov")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehov_totals" }

        assertEquals("behov", godkjenningsbehovløsningTotals.labelValue("forarsaketAvEventName"))
    }

    @Test
    fun `ta med forårsaket_av event_name i godkjenningsbehovmetric 2`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehov("påminnelse")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehov_totals" }

        assertEquals("påminnelse", godkjenningsbehovløsningTotals.labelValue("forarsaketAvEventName"))
    }

    @Test
    fun `ta med forårsaket_av event_name i godkjenningsbehovløsningmetric`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehovløsning("behov")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehovlosning_totals" }

        assertEquals("behov", godkjenningsbehovløsningTotals.labelValue("forarsaketAvEventName"))
    }

    @Test
    fun `ta med forårsaket_av event_name i godkjenningsbehovløsningmetric 2`() {
        val newSamples = forskjellerIMetrikker { rapid.sendTestMessage(godkjenningsbehovløsning("påminnelse")) }
        val godkjenningsbehovløsningTotals = newSamples.single { it.name == "godkjenningsbehovlosning_totals" }

        assertEquals("påminnelse", godkjenningsbehovløsningTotals.labelValue("forarsaketAvEventName"))
    }

    @Test
    fun `skiller mellom opprinnelig godkjenningsbehov og resending som følge av påminnelse`() {
        fun assertMarkertSomResending(tall: String, sample: List<Sample>) =
            assertEquals(
                tall,
                sample.single { it.name == "godkjenningsbehov_totals" }.labelValue("resendingAvGodkjenningsbehov")
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
    ) = """
        {
          "@event_name": "behov",
          "@behov": [
            "Godkjenning"
          ],
          "@forårsaket_av": {
            "behov": [${forårsaketAvBehov.joinToString { """"$it"""" }}],
            "event_name": "$forårsaketAvEventName"
          },
          "tilstand": "AVVENTER_GODKJENNING",
          "vedtaksperiodeId": "$vedtaksperiodeId",
          "Godkjenning": {
            "utbetalingtype": "$utbetalingtype",
            "periodetype": "OVERGANG_FRA_IT",
            "inntektskilde": "FLERE_ARBEIDSGIVERE",
            "warnings": {
              "aktiviteter": [],
              "kontekster": []
            }
          }
        }
    """

    @Language("JSON")
    private fun godkjenningsbehovløsning(forårsaketAvEventName: String = "behov", utbetalingtype: String = "UTBETALING") = """{
      "@behov": [
        "Godkjenning"
      ],
      "@forårsaket_av": {
        "event_name": "$forårsaketAvEventName"
      },
      "Godkjenning": {
        "utbetalingtype": "$utbetalingtype",
        "periodetype": "FORLENGELSE",
        "inntektskilde": "EN_ARBEIDSGIVER",
        "warnings": {
          "aktiviteter": [],
          "kontekster": []
        }
      },
      "@løsning": {
        "Godkjenning": {
          "godkjent": true,
          "automatiskBehandling": true,
          "makstidOppnådd": false
        }
      },
      "@final": true
    } 
    """
}
