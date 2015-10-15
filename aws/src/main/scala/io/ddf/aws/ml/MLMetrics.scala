package io.ddf.aws.ml

import io.ddf.DDF
import io.ddf.exception.DDFException
import io.ddf.jdbc.content.{SqlArrayResult, Representations}
import io.ddf.misc.{Config, ADDFFunctionalGroupHandler}
import io.ddf.ml.{MLSupporter => CoreMLSupporter, _}


class MLMetrics(ddf: DDF) extends io.ddf.ml.AMLMetricsSupporter(ddf) {

  @throws(classOf[DDFException])
  override def r2score(meanYTrue: Double): Double = {
    return 0
  }

  @throws(classOf[DDFException])
  override def residuals: DDF = {
    return null
  }

  @throws(classOf[DDFException])
  override def roc(predictionDDF: DDF, alpha_length: Int): RocMetric = {
    val originalDDF = ddf
    val model = ddf.ML.train("REGRESSION")
    //wait for model
    val predictedDDF: DDF = predictionDDF.ML.applyModel(model)
    val predictDDFAsSql = predictedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
      .asInstanceOf[SqlArrayResult].result
    val originalDDFAsSql = originalDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
      .asInstanceOf[SqlArrayResult].result
    val result = originalDDFAsSql map (row => row(row.size - 1)) zip (predictDDFAsSql map (row => row(row.size - 1)))
    val matrix = result map { case (oldVal, newVal) => Array(oldVal.asInstanceOf[Double], newVal.asInstanceOf[Double]) }
    val rocMetric = new RocMetric(matrix.toArray, 0.0)
    rocMetric.computeAUC()
    rocMetric
  }

  @throws(classOf[DDFException])
  override def rmse(testDDF: DDF, implicitPref: Boolean): Double = {
    val model = ddf.ML.train("REGRESSION")
    val awsModel = model.asInstanceOf[MLModel]
    val sql = "SELECT * FROM " + testDDF.getTableName + "limit 1"
    val datasourceId = AwsModelHelper.createDataSourceFromRedShift(ddf.getSchema, sql, awsModel.getModelType)
    AwsModelHelper.getEvaluationMetrics(datasourceId, awsModel.getModelId, awsModel.getModelType)
  }
}