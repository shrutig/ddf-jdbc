package io.ddf.jdbc.analytics

import io.ddf.DDF
import io.ddf.analytics.{Summary, SimpleSummary, AStatisticsSupporter}

class StatisticsSupporter(ddf:DDF) extends AStatisticsSupporter(ddf){
  override def getSummaryImpl: Array[Summary] = ???

  override def getSimpleSummaryImpl: Array[SimpleSummary] = ???
}
