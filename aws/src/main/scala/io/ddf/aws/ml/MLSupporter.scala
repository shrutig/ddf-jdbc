package io.ddf.aws.ml

import java.lang

import com.amazonaws.services.machinelearning.model.MLModelType
import io.ddf.DDF
import io.ddf.aws.AWSDDFManager
import io.ddf.aws.ml.util.CrossValidation
import io.ddf.jdbc.content.{DdlCommand, Representations, SqlArrayResult}
import io.ddf.misc.ADDFFunctionalGroupHandler
import io.ddf.ml.{CrossValidationSet, IModel, ISupportML}

import scala.collection.JavaConverters._

class MLSupporter(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with ISupportML with Serializable {

  val ddfManager = ddf.getManager.asInstanceOf[AWSDDFManager]
  val awsProperties = AwsConfig.getPropertiesForAmazonML(ddfManager)
  val awsHelper = new AwsHelper(awsProperties.s3Properties)
  val awsMLHelper = new AwsMLHelper(awsProperties)

  def getAwsMLHelper = awsMLHelper

  def getAwsHelper = awsHelper

  override def applyModel(model: IModel): DDF = applyModel(model, hasLabels = true)

  override def applyModel(model: IModel, hasLabels: Boolean): DDF = applyModel(model, hasLabels, includeFeatures = true)

  override def applyModel(model: IModel, hasLabels: Boolean, includeFeatures: Boolean): DDF = {
    val awsModel = model.asInstanceOf[AwsModel]
    val tableName = ddf.getTableName
    val sql = awsHelper.selectSql(tableName)
    val dataSourceId = awsMLHelper.createDataSourceFromRedShift(ddf.getSchema, sql, awsModel.getMLModelType)
    //this will wait for batch prediction to complete
    val batchId = awsMLHelper.createBatchPrediction(awsModel, dataSourceId)
    //last column is target column for supervised learning
    val targetColumn = ddf.getSchema.getColumns.asScala.last
    val newTableName = Identifiers.newTableName(batchId)
    val createTableSql = awsMLHelper.createTableSqlForModelType(awsModel.getMLModelType, newTableName, targetColumn)
    val newDDF = ddf.getManager.asInstanceOf[AWSDDFManager].create(createTableSql)
    //now copy the results to redshift
    val manifestPath = awsHelper.createResultsManifestForRedshift(batchId)
    val sqlToCopy = awsHelper.sqlToCopyFromS3ToRedshift(manifestPath, newTableName)
    implicit val cat = ddfManager.catalog
    DdlCommand(ddfManager.getConnection(), ddfManager.baseSchema, sqlToCopy)
    //and return the ddf
    newDDF
  }

  override def CVRandom(k: Int, trainingSize: Double, seed: lang.Long): java.util.List[CrossValidationSet] = {
    val crossValidation: CrossValidation = new CrossValidation(ddf)
    crossValidation.CVRandom(k, trainingSize, seed)
  }

  override def CVKFold(k: Int, seed: lang.Long): java.util.List[CrossValidationSet] = {
    val crossValidation: CrossValidation = new CrossValidation(ddf)
    crossValidation.CVK(k)
  }

  override def train(trainMethodKey: String, args: AnyRef*): IModel = {
    val sql = awsHelper.selectSql(ddf.getTableName)
    val mlModelType = MLModelType.valueOf(trainMethodKey)

    val dataSourceId = awsMLHelper.createDataSourceFromRedShift(ddf.getSchema, sql, mlModelType)
    val paramsMap = if (args.length < 1 || args(0) == null) {
      new java.util.HashMap[String, String]()
    } else {
      args(0).asInstanceOf[java.util.Map[String, String]]
    }

    //this will wait for model creation to complete
    val modelId = awsMLHelper.createModel(dataSourceId, mlModelType, paramsMap)
    new AwsModel(modelId, mlModelType)
  }

  def getConfusionMatrix(iModel: IModel, v: Double): Array[Array[Long]] = {
    val predictedDDF = ddf.ML.applyModel(iModel)
    val originalDDF = ddf
    val matrix = Array.ofDim[Long](2, 2)

    val predictDDFAsSql = predictedDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
      .asInstanceOf[SqlArrayResult].result
    val originalDDFAsSql = originalDDF.getRepresentationHandler.get(Representations.SQL_ARRAY_RESULT)
      .asInstanceOf[SqlArrayResult].result
    val result = originalDDFAsSql map (row => row(row.length - 1)) zip (predictDDFAsSql map (row => row(row.length - 1)))

    for (row <- result.indices) {
      val newVal = result(row)._2.asInstanceOf[Double]
      val oldVal = (List(result(row)._1) collect { case i: java.lang.Number => i.doubleValue() }).sum
      if (oldVal < v && newVal < v)
        matrix(0)(0) = matrix(0)(0) + 1
      else if (oldVal < v && newVal > v)
        matrix(0)(1) = matrix(0)(1) + 1
      else if (oldVal > v && newVal < v)
        matrix(1)(0) = matrix(1)(0) + 1
      else
        matrix(1)(1) = matrix(1)(1) + 1
    }
    matrix
  }


}
