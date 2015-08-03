package io.ddf.jdbc.analytics

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

}
