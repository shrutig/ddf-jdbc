package io.ddf.aws.ml

import io.ddf.DDF
import io.ddf.exception.DDFException
import io.ddf.misc.{Config, ADDFFunctionalGroupHandler}
import io.ddf.ml.{MLSupporter => CoreMLSupporter, _}


class MLMetrics(ddf: DDF) extends AMLMetricsSupporter(ddf) {

  @throws(classOf[DDFException])
  override def r2score(meanYTrue: Double): Double = {
    val model = ddf.ML.train("BINARY")
    val result = ddf.ML.applyModel(model)

    return 0
  }

  @throws(classOf[DDFException])
  override def residuals: DDF = {
    return null
  }

  @throws(classOf[DDFException])
  override def roc(predictionDDF: DDF, alpha_length: Int): RocMetric = {
    return null
  }

  @throws(classOf[DDFException])
  override def rmse(testDDF: DDF, implicitPref: Boolean): Double = {
    val model = ddf.ML.train("REGRESSION")
    val sql = "SELECT * FROM " + testDDF.getTableName
    val datasourceId = AwsModelHelper.createDataSourceFromRedShift(sql)
    val evaluationId = AwsModelHelper.createEvaluation(model.getRawModel.toString,datasourceId)

    return 0
  }
}