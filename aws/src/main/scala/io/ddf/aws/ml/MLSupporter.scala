
package io.ddf.aws.ml

import com.amazonaws.services.machinelearning.model.MLModelType
import io.ddf.DDF
import io.ddf.aws.AWSDDFManager
import io.ddf.content.Schema
import io.ddf.jdbc.{JdbcDDFManager, JdbcDDF}
import io.ddf.jdbc.content.{SqlArrayResult, Representations, DdlCommand}
import io.ddf.misc.{ADDFFunctionalGroupHandler, Config}
import io.ddf.ml.{CrossValidationSet, IModel, ISupportML}
import java.{util, lang}
import scala.reflect.runtime.{universe => ru}


class MLSupporter(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with ISupportML with Serializable {

  val RECIPE = "recipe"

  override def train(trainMethodKey: String, args: AnyRef*): IModel = {
    val sql = "SELECT * FROM " + ddf.getTableName
    val datasourceId = AwsModelHelper.createDataSourceFromRedShift(ddf.getSchema, sql, trainMethodKey)
    val arguments = if (args.isEmpty) null else args.asInstanceOf[java.util.Map[String, String]]
    val modelId = AwsModelHelper.createModel(datasourceId, Config.getValue(ddf.getEngine, "recipe"), MLModelType.valueOf
      (trainMethodKey), arguments)
    new MLModel(Seq(modelId, trainMethodKey))
  }

  override def applyModel(model: IModel): DDF = applyModel(model, true)

  override def applyModel(model: IModel, hasLabels: Boolean): DDF = applyModel(model, hasLabels, true)

  override def applyModel(model: IModel, hasLabels: Boolean, includeFeatures: Boolean): DDF = {
    val awsModel = model.asInstanceOf[MLModel]
    val sql = "SELECT * FROM " + ddf.getTableName
    val datasourceId = AwsModelHelper.createDataSourceFromRedShift(ddf.getSchema, sql, awsModel.getModelType)
    awsModel.predictDataSource(ddf, datasourceId)
  }

  def getConfusionMatrix(iModel: IModel, v: Double): Array[Array[Long]] = {
    val predictedDDF = ddf.ML.applyModel(iModel)
    val originalDDF = ddf
    val matrix = Array.ofDim[Long](2, 2)
    /*val targetColumn = originalDDF.getColumnNames.get(originalDDF.getColumnNames.size()-1)*/
    val predictDDFAsSql = predictedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
      .asInstanceOf[SqlArrayResult].result
    val originalDDFAsSql = originalDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
      .asInstanceOf[SqlArrayResult].result
    val result = originalDDFAsSql map (row => row(row.size - 1)) zip (predictDDFAsSql map (row => row(row.size -
      1)))

    for (row <- 0 to result.size - 1 ) {
      val newVal = result(row)._2.asInstanceOf[Double]
      val oldVal =( List(result(row)._1) collect { case i: java.lang.Number => i.doubleValue() } ). sum
      if(oldVal < v && newVal < v)
        matrix(0)(0) = matrix(0)(0) + 1
      else if(oldVal < v && newVal > v)
        matrix(0)(1) = matrix(0)(1) + 1
      else if(oldVal > v && newVal < v)
        matrix(1)(0) = matrix(1)(0) + 1
      else
        matrix(1)(1) = matrix(1)(1) + 1
    }
    return matrix
  }

  lazy val crossValidation: CrossValidation = new CrossValidation(ddf)

  def CVRandom(k: Int, trainingSize: Double, seed: lang.Long): util.List[CrossValidationSet] = {
    crossValidation.CVRandom(k, trainingSize, seed)
  }

  def CVKFold(k: Int, seed: lang.Long): util.List[CrossValidationSet] = {
    crossValidation.CVK(k, seed)
  }

}
