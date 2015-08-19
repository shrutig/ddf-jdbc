package io.ddf.postgres.analytics

import com.google.common.base.Strings
import io.ddf.DDF

class StatisticsHandler(ddf:DDF) extends io.ddf.jdbc.analytics.StatisticsHandler(ddf){

  override def getVectorCor(xColumnName: String, yColumnName: String): Double = {
    var corr: Double = 0.0
    val command: String = String.format("select corr(%s, %s) from @this", xColumnName, yColumnName)
    if (!Strings.isNullOrEmpty(command)) {
      val result: java.util.List[String] = this.getDDF.sql(command, String.format("Unable to compute correlation of %s and %s from table %%s", xColumnName, yColumnName)).getRows
      if (result != null && !result.isEmpty && result.get(0) != null) {
        corr = result.get(0).toDouble
        return corr
      }
    }
    return Double.NaN
  }

  override def getVectorCovariance(xColumnName: String, yColumnName: String): Double = {
    var cov: Double = 0.0
    val command: String = String.format("select  covar_samp(%s, %s) from @this", xColumnName, yColumnName)
    if (!Strings.isNullOrEmpty(command)) {
      val result: java.util.List[String] = this.getDDF.sql(command, String.format("Unable to compute covariance of %s and %s from table %%s", xColumnName, yColumnName)).getRows
      if (result != null && !result.isEmpty && result.get(0) != null) {
        System.out.println(">>>>> parseDouble: " + result.get(0))
        cov = result.get(0).toDouble
        return cov
      }
    }
    return Double.NaN
  }
}
