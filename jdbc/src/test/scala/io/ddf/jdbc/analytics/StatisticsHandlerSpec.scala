package io.ddf.jdbc.analytics

import io.ddf.analytics.AStatisticsSupporter
import io.ddf.jdbc.BaseSpec

class StatisticsHandlerSpec extends BaseSpec {
  val ddf = loadAirlineNADDF()

  it should "calculate summary and simple summary" in {

    val summaries = ddf.getSummary
    summaries.head.max() should be(2010)

    val randomSummary = summaries(8)
    randomSummary.variance() should be(998284.8 +- 1.0)

  }
  it should "calculate vector mean" in {
    ddf.getVectorMean("YEAR") should not be null
  }

  it should "calculate vector variance" in {
    val variance = ddf.getVectorVariance("YEAR")
    variance.length should be(2)
  }

  it should "calculate vector cor" in {
    val cor = ddf.getVectorCor("YEAR", "MONTH")
    println(cor)
    cor should not be null
  }

  it should "calculate vector covariance" in {
    val cov = ddf.getVectorCovariance("YEAR", "MONTH")
    println(cov)
    cov should not be null
  }

  it should "calculate vector quantiles" in {
    val pArray: Array[java.lang.Double] = Array(0.3, 0.5, 0.7)
    val expectedQuantiles: Array[java.lang.Double] = Array(801.0, 1416.0, 1644.0)
    val quantiles: Array[java.lang.Double] = ddf.getVectorQuantiles("deptime", pArray)
    quantiles should equal(expectedQuantiles)
  }

  it should "calculate vector histogram" in {
    val bins: java.util.List[AStatisticsSupporter.HistogramBin] = ddf.getVectorHistogram("arrdelay", 5)
    bins.size should be(5)
    val first = bins.get(0)
    first.getX should be(-24)
    first.getY should be(10.0)
  }
}
