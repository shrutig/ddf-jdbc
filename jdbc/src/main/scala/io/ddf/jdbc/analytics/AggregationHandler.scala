package io.ddf.jdbc.analytics

import io.ddf.DDF
import io.ddf.jdbc.analytics.StatsUtils.PearsonCorrelation
import io.ddf.jdbc.content.{Representations, SqlArrayResult}

class AggregationHandler(ddf: DDF) extends io.ddf.analytics.AggregationHandler(ddf) {

  override def computeCorrelation(columnA: String, columnB: String): Double = {
    val rowDataSet = ddf.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT).asInstanceOf[SqlArrayResult].result
    val colAIndex = ddf.getColumnIndex(columnA)
    val colBIndex = ddf.getColumnIndex(columnB)

    val intermediateResult = rowDataSet.map { row =>
      val colAVal = row(colAIndex)
      val colBVal = row(colBIndex)
      val colAElement = if (colAVal == null) 0 else colAVal.toString.toDouble
      val colBElement = if (colBVal == null) 0 else colBVal.toString.toDouble
      new PearsonCorrelation(colAElement, colBElement,
        colAElement * colBElement, colAElement * colAElement,
        colBElement * colBElement, 1)
    }.reduce(_.merge(_))

    intermediateResult.evaluate
  }

}
