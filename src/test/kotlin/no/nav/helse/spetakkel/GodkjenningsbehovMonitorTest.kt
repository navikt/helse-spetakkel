package no.nav.helse.spetakkel

import io.prometheus.client.Collector
import io.prometheus.client.CollectorRegistry
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class GodkjenningsbehovMonitorTest {

    val rapid = TestRapid()

    init {
        GodkjenningsbehovMonitor(rapid)
    }

    @BeforeEach
    fun clear() {
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

    private fun Collector.MetricFamilySamples.Sample.labelValue(labelName: String): String {
        assertTrue (labelName in labelNames, "Kan ikke finne metrikk med label $labelName i $labelNames")
        return labelValues[labelNames.indexOf(labelName)]
    }

    private fun forskjellerIMetrikker(block: () -> Unit): List<Collector.MetricFamilySamples.Sample> {
        val before = CollectorRegistry.defaultRegistry.metricFamilySamples().toList().flatMap { it.samples }
        block()
        val after = CollectorRegistry.defaultRegistry.metricFamilySamples().toList().flatMap { it.samples }

        return after.map { valueAfter ->
            val valueBefore = before.find { it.name == valueAfter.name && it.labelValues == valueAfter.labelValues }
                ?: return@map valueAfter
            Collector.MetricFamilySamples.Sample(
                valueAfter.name,
                valueAfter.labelNames,
                valueAfter.labelValues,
                valueAfter.value - valueBefore.value
            )
        }.filter { it.value != 0.0 }
    }

    @Language("JSON")
    private fun godkjenningsbehov(forårsaketAvEventName: String = "behov") = """
        {
          "@event_name": "behov",
          "@behov": [
            "Godkjenning"
          ],
          "@forårsaket_av": {
            "event_name": "$forårsaketAvEventName"
          },
          "tilstand": "AVVENTER_GODKJENNING",
          "Godkjenning": {
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
    private fun godkjenningsbehovløsning(forårsaketAvEventName: String = "behov") = """{
      "@behov": [
        "Godkjenning"
      ],
      "@forårsaket_av": {
        "event_name": "$forårsaketAvEventName"
      },
      "Godkjenning": {
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
