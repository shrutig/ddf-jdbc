package io.ddf.aws.ml

import java.{lang, util}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.machinelearning.model.{MLModelType, RedshiftDatabase, RedshiftDatabaseCredentials}
import io.ddf.DDF
import io.ddf.aws.AWSDDFManager
import io.ddf.aws.ml.util.CrossValidation
import io.ddf.jdbc.content.DdlCommand
import io.ddf.misc.ADDFFunctionalGroupHandler
import io.ddf.ml.{CrossValidationSet, IModel, ISupportML}

import scala.collection.JavaConverters._

class MLSupporter(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with ISupportML with Serializable {

  val ddFManager = ddf.getManager.asInstanceOf[AWSDDFManager]
  val credentials = ddFManager.credentials
  val runtimeConfig = ddFManager.getRuntimeConfig
  val awsHelper = new AwsHelper(getPropertiesForAmazonML)


  def getPropertiesForAmazonML = {
    val accessId = ddFManager.getRequiredValue("awsAccessId")
    val accessKey = ddFManager.getRequiredValue("awsAccessKey")
    val redshiftDatabaseName = ddFManager.getRequiredValue("redshiftDatabase")
    val redshiftClusterId = ddFManager.getRequiredValue("redshiftClusterId")
    val roleArn = ddFManager.getRequiredValue("redshiftIAMRoleARN")
    val s3StagingBucket = ddFManager.getRequiredValue("s3StagingBucket")
    val s3Region = ddFManager.getRequiredValue("s3Region")

    val awsCredentials = new BasicAWSCredentials(accessId, accessKey)
    val redshiftDatabase = new RedshiftDatabase().withDatabaseName(redshiftDatabaseName).withClusterIdentifier(redshiftClusterId)
    val redshiftDatabaseCredentials = new RedshiftDatabaseCredentials().withUsername(credentials.getUsername).withPassword(credentials.getPassword)
    new AwsProperties(awsCredentials, redshiftDatabase, redshiftDatabaseCredentials, credentials, s3StagingBucket, s3Region, roleArn)
  }

  override def applyModel(model: IModel): DDF = applyModel(model, hasLabels = true)

  override def applyModel(model: IModel, hasLabels: Boolean): DDF = applyModel(model, hasLabels, includeFeatures = true)

  override def applyModel(model: IModel, hasLabels: Boolean, includeFeatures: Boolean): DDF = {
    val awsModel = model.asInstanceOf[AwsModel]
    val tableName = Identifiers.newTableName(awsModel.getModelId)
    val dataSourceId = awsHelper.createDataSourceFromRedShift(ddf.getSchema, s"SELECT * FROM $tableName")
    //this will wait for batch prediction to complete
    val batchId = awsHelper.createBatchPrediction(awsModel, dataSourceId)
    //last column is target column for supervised learning
    val targetColumn = ddf.getSchema.getColumns.asScala.last
    val createTableSql = awsHelper.createTableSqlForModelType(awsModel.getMLModelType, tableName, targetColumn)
    val newDDF = ddf.getManager.asInstanceOf[AWSDDFManager].create(createTableSql)
    //now copy the results to redshift
    val manifestPath = awsHelper.createResultsManifestForRedshift(batchId)
    val sqlToCopy = awsHelper.sqlToCopyFromS3ToRedshift(manifestPath, tableName)
    implicit val cat = ddFManager.catalog
    DdlCommand(ddFManager.getConnection(), ddFManager.baseSchema, sqlToCopy)
    //and return the ddf
    newDDF
  }

  override def CVRandom(k: Int, trainingSize: Double, seed: lang.Long): util.List[CrossValidationSet] = {
    val crossValidation: CrossValidation = new CrossValidation(ddf)
    crossValidation.CVRandom(k, trainingSize, seed)
  }

  override def CVKFold(k: Int, seed: lang.Long): util.List[CrossValidationSet] = {
    val crossValidation: CrossValidation = new CrossValidation(ddf)
    crossValidation.CVK(k, seed)
  }


  override def train(trainMethodKey: String, args: AnyRef*): IModel = {
    val sql = "SELECT * FROM " + ddf.getTableName
    val dataSourceId = awsHelper.createDataSourceFromRedShift(ddf.getSchema, sql)
    val paramsMap = if (args.length < 1 || args(0) == null) {
      new util.HashMap[String, String]()
    } else {
      args(0).asInstanceOf[java.util.Map[String, String]]
    }
    val mlModelType = MLModelType.valueOf(trainMethodKey)
    //this will wait for model creation to complete
    val modelId = awsHelper.createModel(dataSourceId, mlModelType, paramsMap)
    new AwsModel(modelId, mlModelType)
  }

  override def getConfusionMatrix(model: IModel, threshold: Double): Array[Array[Long]] = {
    null
  }


}
