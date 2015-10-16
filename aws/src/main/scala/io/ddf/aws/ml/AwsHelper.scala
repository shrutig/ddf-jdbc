package io.ddf.aws.ml


import java.io._
import java.util.Date
import java.util.concurrent.TimeUnit

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.machinelearning.AmazonMachineLearningClient
import com.amazonaws.services.machinelearning.model._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}
import io.ddf.content.Schema
import io.ddf.content.Schema.ColumnClass
import io.ddf.datasource.JDBCDataSourceCredentials
import io.ddf.exception.DDFException
import io.ddf.jdbc.content.{Representations, TableNameRepresentation}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.util.Random

class AwsHelper(awsProperties: AwsProperties) {

  val client = new AmazonMachineLearningClient(awsProperties.credentials)
  val s3Client = new AmazonS3Client(awsProperties.credentials)

  def createModel(trainDataSourceId: String, modelType: MLModelType, parameters: java.util.Map[String,
    String]): String = {
    val modelId = Identifiers.newMLModelId
    val request = new CreateMLModelRequest()
      .withMLModelId(modelId)
      .withMLModelName(modelId)
      .withMLModelType(modelType)
      .withTrainingDataSourceId(trainDataSourceId)
    if (parameters != null) {
      request.withParameters(parameters)
    }
    client.createMLModel(request)
    //wait for this to complete
    waitForModel(modelId)
    modelId
  }


  def createDataSourceFromRedShift(schema: Schema, sqlQuery: String): String = {
    val dataSourceId = Identifiers.newDataSourceId
    val dataSpec = new RedshiftDataSpec()
      .withDatabaseInformation(awsProperties.redshiftDatabase)
      .withDatabaseCredentials(awsProperties.redshiftDatabaseCredentials)
      .withSelectSqlQuery(sqlQuery)
      .withS3StagingLocation(awsProperties.s3StagingBucket)
      .withDataSchema(getSchemaAttributeDataSource(schema))
    val request = new CreateDataSourceFromRedshiftRequest()
      .withComputeStatistics(true)
      .withDataSourceId(dataSourceId)
      .withDataSpec(dataSpec)
      .withRoleARN(awsProperties.mlIAMRoleARN)
    client.createDataSourceFromRedshift(request)
    dataSourceId
  }

  def getSchemaAttributeDataSource(schema: Schema): String = {
    val columns = schema.getColumns.asScala
    //for supervised learning last column is the target column
    val target = columns.last.getName
    val listColumns = columns map (u => (u.getName, attributeTypeFromColumnClass(u.getColumnClass)))
    val attributes = listColumns.map { case (name, attrType) =>
      "{\"attributeName\":" + "\"" + name + "\",\"attributeType\": \"" + attrType + "\"\n}"
    }.mkString(",")

    "{\n  \"excludedAttributeNames\": [],\n  \"version\": \"1.0\",\n " +
      " \"dataFormat\": \"CSV\",\n  \"rowId\": null,\n  \"dataFileContainsHeader\": true,\n" +
      "  \"attributes\": [" + attributes + "],\n  \"targetAttributeName\": \"" + target + "\"\n}"
  }

  def attributeTypeFromColumnClass(columnClass: ColumnClass): String = {
    columnClass match {
      case ColumnClass.LOGICAL => "BINARY"
      case ColumnClass.NUMERIC => "NUMERIC"
      case ColumnClass.FACTOR => "CATEGORICAL"
      case ColumnClass.CHARACTER => "TEXT"
    }
  }

  def createTableSqlForModelType(mLModelType: MLModelType, tableName: String, targetColumn: Schema.Column): String = {
    mLModelType match {
      case MLModelType.BINARY => s"CREATE TABLE $tableName (bestAnswer int4,score float8)"
      case MLModelType.REGRESSION => s"CREATE TABLE $tableName (score float8)"
      case MLModelType.MULTICLASS =>
        val columns = targetColumn.getOptionalFactor.getLevels.asScala.mkString(",")
        s"CREATE TABLE $tableName ($columns)"
    }
  }


  def createBatchPrediction(awsModel: AwsModel, dataSourceId: String): String = {
    val batchPredictionId = Identifiers.newBatchPredictionId
    val bpRequest = new CreateBatchPredictionRequest()
      .withBatchPredictionId(batchPredictionId)
      .withBatchPredictionName("Batch Prediction for " + dataSourceId)
      .withMLModelId(awsModel.getModelId)
      .withOutputUri(awsProperties.s3StagingBucket)
      .withBatchPredictionDataSourceId(dataSourceId)
    client.createBatchPrediction(bpRequest)
    //wait for this to complete
    waitFoBatchPrediction(batchPredictionId)
    batchPredictionId
  }

  def createResultsManifestForRedshift(batchId: String): String = {
    val newManifest: String = makeModifiedManifestString(batchId)
    uploadStringToS3(newManifest, batchId + ".manifest", awsProperties.s3StagingBucket)
    val url = "s3://" + awsProperties.s3StagingBucket + "/batch-prediction/" + batchId + ".manifest"
    url
  }

  /*
   * We are making a modified manifest as Redshift does not like the one that AWS/ML made. Stupid but true!
   */
  def makeModifiedManifestString(batchId: String): String = {
    val obj: InputStream = s3Client.getObject(awsProperties.s3StagingBucket, "batch-prediction/" +
      batchId + ".manifest") getObjectContent()
    val oldManifest = Source.fromInputStream(obj).mkString
    val allEntries = oldManifest.trim.stripPrefix("{").stripSuffix("}")
    val manifestEntries = allEntries.split(",").map(u => u.split("\":\"")(1)).filterNot(u => u.endsWith("tmp.gz\""))
    val modifiedManifest: String = manifestEntries.map(u => "{\"url\":" + "\"" + u + "},").mkString
    val newManifest = "{\"entries\":[" + modifiedManifest.stripSuffix(",") + "]}"
    newManifest
  }

  def uploadStringToS3(content: String, key: String, bucketName: String) {
    val contentAsBytes = content.getBytes("UTF-8")
    val contentsAsStream = new ByteArrayInputStream(contentAsBytes)
    val md = new ObjectMetadata()
    md.setContentLength(contentAsBytes.length)
    val objectRequest = new PutObjectRequest(bucketName, key, contentsAsStream, md)
    s3Client.putObject(objectRequest)
  }

  def sqlToCopyFromS3ToRedshift(s3Source: String, tableName: String) = {
    val id = awsProperties.credentials.getAWSAccessKeyId
    val key = awsProperties.credentials.getAWSSecretKey
    val region = awsProperties.s3Region
    val copySql = s"COPY $tableName from '$s3Source' credentials "
    val accessInfo = s"'aws_access_key_id=$id;aws_secret_access_key=$key'"
    s"$copySql $accessInfo manifest  delimiter ',' REGION '$region' gzip IGNOREHEADER 1 ;"
  }


  def waitForModel(modelId: String) = {
    waitForCompletion(modelId, "MODEL")
  }

  def waitFoBatchPrediction(batchId: String) = {
    waitForCompletion(batchId, "BATCH_PREDICTION")
  }

  def waitForEvaluation(evaluationId: String) = {
    waitForCompletion(evaluationId, "EVALUATION")
  }


  def waitForCompletion(entityId: String, entityType: String, delay: FiniteDuration = FiniteDuration(2, TimeUnit.SECONDS)): Unit = {
    var terminated = false
    def getStatus = {
      entityType match {
        case "MODEL" => val request = new GetMLModelRequest().withMLModelId(entityId)
          client.getMLModel(request) getStatus
        case "EVALUATION" => val request = new GetEvaluationRequest().withEvaluationId(entityId)
          client.getEvaluation(request) getStatus
        case "BATCH_PREDICTION" => val request = new GetBatchPredictionRequest().withBatchPredictionId(entityId)
          client.getBatchPrediction(request) getStatus
      }
    }
    while (!terminated) {
      val status = getStatus
      println(s"Entity $entityId of type $entityType is $status at ${new Date()}")
      status match {
        case "COMPLETED" | "FAILED" | "INVALID" => terminated = true
        case _ => terminated = false
      }
      try if (!terminated) Thread.sleep(delay.toMillis)
      catch {
        case e: InterruptedException => throw new DDFException(e)
      }
    }
  }

  def getEvaluationMetrics(dataSourceId: String, modelId: String, modelType: String): Double = {
    val evalId = Identifiers.newEvaluationId
    val request = new CreateEvaluationRequest()
      .withMLModelId(modelId)
      .withEvaluationDataSourceId(dataSourceId)
      .withEvaluationId(evalId)
    client.createEvaluation(request)
    val metricRequest = new GetEvaluationRequest()
      .withEvaluationId(evalId)
    val parameter = modelType match {
      case "BINARY" => "BinaryAUC"
      case "REGRESSION" => "RegressionRMSE"
    }
    waitForEvaluation(evalId)
    val answer = client.getEvaluation(metricRequest).getPerformanceMetrics getProperties()
    answer get parameter toDouble
  }

  def selectSql(tableName:String) = s"SELECT * FROM $tableName"
}

object Identifiers {
  private val BASE62_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray

  private def base62RandomChar(): Char = BASE62_CHARS(Random.nextInt(BASE62_CHARS.length))

  private def generateEntityId(prefix: String): String = {
    val rand = List.fill(11)(base62RandomChar()).mkString
    s"${prefix}_${rand}"
  }

  def newDataSourceId: String = generateEntityId("ds")

  def newMLModelId: String = generateEntityId("ml")

  def newEvaluationId: String = generateEntityId("ev")

  def newBatchPredictionId: String = generateEntityId("bp")

  def newTableName(model: String): String = generateEntityId(model)

  def newManifestId: String = generateEntityId("manifest")

  val representation: Array[Class[TableNameRepresentation]] = Array(Representations.VIEW)

}

case class AwsProperties(credentials: BasicAWSCredentials,
                         redshiftDatabase: RedshiftDatabase,
                         redshiftDatabaseCredentials: RedshiftDatabaseCredentials,
                         jdbcCredentials: JDBCDataSourceCredentials,
                         s3StagingBucket: String,
                         s3Region: String,
                         mlIAMRoleARN: String)
