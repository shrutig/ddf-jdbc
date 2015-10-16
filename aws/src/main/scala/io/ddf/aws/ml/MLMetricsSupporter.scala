package io.ddf.aws.ml

import io.ddf.DDF
import io.ddf.exception.DDFException
import io.ddf.jdbc.content.{Representations, SqlArrayResult}
import io.ddf.ml.RocMetric


class MLMetricsSupporter(ddf: DDF) extends io.ddf.ml.AMLMetricsSupporter(ddf) {

  //we depend on aws ML Supporter. This metrics supporter cannot function independently
  val mlSupporter = ddf.getMLSupporter.asInstanceOf[MLSupporter]
  val awsHelper = mlSupporter.getAwsHelper

  @throws(classOf[DDFException])
  override def r2score(meanYTrue: Double): Double = {
    throw new DDFException("Cannot get r2Score", new UnsupportedOperationException())
  }

  @throws(classOf[DDFException])
  override def residuals: DDF = {
    throw new DDFException("Cannot get residuals", new UnsupportedOperationException())
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
    val matrix = result map { case (oldVal, newVal) => Array((List(oldVal) collect { case i: java.lang.Number => i
      .doubleValue()
    }).sum, newVal.asInstanceOf[Double])
    }
    val rocMetric = new RocMetric(matrix.toArray, 0.0)
    rocMetric.computeAUC()
    rocMetric
  }

  @throws(classOf[DDFException])
  override def rmse(testDDF: DDF, implicitPref: Boolean): Double = {
    val model = ddf.ML.train("REGRESSION")
    val awsModel = model.asInstanceOf[AwsModel]
    val tableName = testDDF.getTableName
    val dataSourceId = awsHelper.createDataSourceFromRedShift(ddf.getSchema, s"SELECT * FROM $tableName")
    awsHelper.getEvaluationMetrics(dataSourceId, awsModel.getModelId, awsModel.getMLModelType.toString)
  }
}
