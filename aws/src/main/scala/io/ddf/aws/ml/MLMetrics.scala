package io.ddf.aws.ml

import io.ddf.DDF
import io.ddf.exception.DDFException
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
    val model = ddf.ML.train("BINARY")
    val awsModel = model.asInstanceOf[MLModel]
    val sql = "SELECT * FROM " + predictionDDF.getTableName + "limit 1"
    val datasourceId = AwsModelHelper.createDataSourceFromRedShift(ddf.getSchema,sql,awsModel.getModelType)
    val area = AwsModelHelper.getEvaluationMetrics(datasourceId,awsModel.getModelId,awsModel.getModelType)
    /*new RocMetric(,area)*/
    return null
  }

  @throws(classOf[DDFException])
  override def rmse(testDDF: DDF, implicitPref: Boolean): Double = {
    val model = ddf.ML.train("REGRESSION")
    val awsModel = model.asInstanceOf[MLModel]
    val sql = "SELECT * FROM " + testDDF.getTableName + "limit 1"
    val datasourceId = AwsModelHelper.createDataSourceFromRedShift(ddf.getSchema,sql,awsModel.getModelType)
    AwsModelHelper.getEvaluationMetrics(datasourceId,awsModel.getModelId,awsModel.getModelType)
  }
}