package io.ddf.aws.ml

import java.io.{FileWriter, BufferedWriter, InputStream, File}
import java.sql.{PreparedStatement, Connection}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.machinelearning.AmazonMachineLearningClient
import com.amazonaws.services.machinelearning.model._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.PutObjectRequest
import io.ddf.DDF
import io.ddf.aws.AWSDDFManager
import io.ddf.content.Schema
import io.ddf.content.Schema.ColumnClass
import io.ddf.misc.Config

import scala.io.Source
import scala.collection.JavaConverters._

object AwsModelHelper {
  val PREFIX = "Scala aws ml"
  val AWS = "aws"
  val credentials = new BasicAWSCredentials(Config.getValue(AWS, "accessId"),
    Config.getValue(AWS, "accessKey"))
  val client = new AmazonMachineLearningClient(credentials)
  val JDBC_NAME = "jdbcName"
  val CLUSTER_NAME = "clusterName"
  val DB_USER = "jdbcUser"
  val DB_PASS = "jdbcPassword"
  val ML_STAGE = "mlStage"
  val ML_ROLE_ARN = "mlRoleArn"
  val SCHEMA = "dataSchema"
  val RECIPE = "recipe"

  def copyFromS3(ddf: DDF, s3Source: String, region: String, tableName: String, isManifest: Boolean) = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    val id = Config.getValue(AWS, "accessId")
    val key = Config.getValue(AWS, "accessKey")
    val SQL_COPY = s"COPY $tableName from '$s3Source' credentials "
    val accessInfo = s"'aws_access_key_id=$id;aws_secret_access_key=$key'"
    val sql = if (isManifest) SQL_COPY + accessInfo + s" manifest  delimiter ',' REGION '$region' gzip IGNOREHEADER 1 ;"
    else SQL_COPY + accessInfo + s" delimiter ',' REGION '$region';"
    try {
      connection = ddf.getManager.asInstanceOf[AWSDDFManager].getConnection
      preparedStatement = connection.prepareStatement(sql)
      preparedStatement.executeUpdate()
    }
    catch {
      case e: Exception => {
        throw new Exception("copy from S3 failed", e)
      }
    }
    finally {
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }
  }

  def getNewManifestPath(batchId: String): String = {
    val obj: InputStream = s3Client.getObject(Config.getValue(AWS, "bucketName"), "batch-prediction/" +
      "bp-tnDZRqdDHxy" + ".manifest") getObjectContent()
    /*val obj: InputStream = s3Client.getObject(Config.getValue(AWS, "bucketName"), Config.getValue(AWS, "key")
      + "/batch-prediction/result" + batchId + ".manifest") getObjectContent()*/
    val oldManifest = Source.fromInputStream(obj).mkString
    obj.close()
    val modifiedManifest: String = oldManifest.trim.stripPrefix("{").stripSuffix("}").split(",") map (u => u
      .split("\":\"")(1)) filterNot (u => u.endsWith("tmp.gz\"")) map (u => "{\"url\":" + "\"" + u + "},") mkString
    val newManifest = "{\"entries\":[" + modifiedManifest.stripSuffix(",") + "]}"
    val fileName = Identifiers.newManifestId
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(newManifest)
    bw.close()
    uploadToS3(fileName, Config.getValue(AWS, "key") + fileName + ".manifest", Config.getValue(AWS,
      "bucketName"))
    "s3://" + Config.getValue(AWS,
      "bucketName") + "/" + Config.getValue(AWS, "key") + fileName + ".manifest"
  }

  lazy val s3Client = new AmazonS3Client(credentials)

  def uploadToS3(filePath: String, fileId: String, bucketName: String) {
    val s3Client = new AmazonS3Client(credentials)
    val file = new File(filePath)
    val objectRequest = new PutObjectRequest(bucketName, fileId, file)
    s3Client.putObject(objectRequest)
  }

  def createDataSourceFromRedShift(schema: Schema, sqlQuery: String,modelType:String): String = {
    val entityId = Identifiers.newDataSourceId
    val database = new RedshiftDatabase()
      .withDatabaseName(Config.getValue(AWS, JDBC_NAME))
      .withClusterIdentifier(Config.getValue(AWS, CLUSTER_NAME))
    val databaseCredentials = new RedshiftDatabaseCredentials()
      .withUsername(Config.getValue(AWS, DB_USER))
      .withPassword(Config.getValue(AWS, DB_PASS))
    val dataSpec = new RedshiftDataSpec()
      .withDatabaseInformation(database)
      .withDatabaseCredentials(databaseCredentials)
      .withSelectSqlQuery(sqlQuery)
      .withS3StagingLocation(Config.getValue(AWS, ML_STAGE))
      .withDataSchema(getSchemaAttributeDataSource(schema,modelType))
    val request = new CreateDataSourceFromRedshiftRequest()
      .withComputeStatistics(true)
      .withDataSourceId(entityId)
      .withDataSpec(dataSpec)
      .withRoleARN(Config.getValue(AWS, ML_ROLE_ARN))
    client.createDataSourceFromRedshift(request)
    entityId
  }

  def getEvaluationMetrics(datasourceId:String,modelId:String,modelType: String):Double={
    val evalId = Identifiers.newEvaluationId
    val request = new CreateEvaluationRequest()
    .withMLModelId(modelId)
    .withEvaluationDataSourceId(datasourceId)
    .withEvaluationId(evalId)
    client.createEvaluation(request)
    val metricRequest = new GetEvaluationRequest()
    .withEvaluationId(evalId)
    val parameter = modelType match {
      case "BINARY" => "BinaryAUC"
      case "REGRESSION" => "RegressionRMSE"
    }
    val answer = client.getEvaluation(metricRequest) .getPerformanceMetrics
      .asInstanceOf[Map[String,String]] get parameter
    answer.asInstanceOf[Double]
  }

  def createModel(trainDataSourceId: String, recipe: String, modelType: MLModelType, parameters: java.util.Map[String,
    String]):
  String = {
    val mlModelId = Identifiers.newMLModelId
    val request = new CreateMLModelRequest()
      .withMLModelId(mlModelId)
      .withMLModelName(PREFIX + " model")
      .withMLModelType(modelType)
      .withRecipe(Source.fromInputStream(getClass.getResourceAsStream(recipe)).mkString)
      .withTrainingDataSourceId(trainDataSourceId)
    if (parameters != null) {
      request.withParameters(parameters)
    }
    client.createMLModel(request)
    mlModelId
  }


  def createEvaluation(mlModelId: String, testDataSourceId: String): String = {
    val evaluationId = Identifiers.newEvaluationId
    val request = new CreateEvaluationRequest()
      .withEvaluationDataSourceId(testDataSourceId)
      .withEvaluationId(evaluationId)
      .withEvaluationName(PREFIX + " evaluation")
      .withMLModelId(mlModelId)
    client.createEvaluation(request)
    evaluationId
  }


  def createBatchPrediction(mlModelId: String, dataSourceId: String, s3OutputUrl: String): String = {
    val batchPredictionId = Identifiers.newBatchPredictionId
    val bpRequest = new CreateBatchPredictionRequest()
      .withBatchPredictionId(batchPredictionId)
      .withBatchPredictionName("Batch Prediction")
      .withMLModelId(mlModelId)
      .withOutputUri(s3OutputUrl)
      .withBatchPredictionDataSourceId(dataSourceId)
    client.createBatchPrediction(bpRequest)
    batchPredictionId
  }

  def predict(ddf: DDF, var1: Array[Double], modelId: String): Double = {
    val prediction = new PredictRequest()
    val predictEndPoint = client.createRealtimeEndpoint(new CreateRealtimeEndpointRequest().withMLModelId(modelId))
    val columns = ddf.getColumnNames.toArray()
    val pair = columns zip var1
    pair foreach (u => prediction.addRecordEntry(u._1.toString, u._2.toString))
    prediction.setMLModelId(modelId)
    prediction.setPredictEndpoint(predictEndPoint.getRealtimeEndpointInfo.getEndpointUrl)
    val predictResult = client.predict(prediction)
    predictResult.getPrediction.getPredictedValue.toDouble
  }

  def getSchemaAttributeDataSource(schema: Schema, modelType: String): String = {
    val columns = schema.getColumns.asScala

    val targetType = modelType match {
      case "BINARY" => "BINARY"
      case "MULTICLASS" => "CATEGORICAL"
      case "REGRESSION" => "NUMERIC"
    }

    //val target = columns.last getName
    val target = if(targetType equals "BINARY") "am" else "depdelay"
    //TODO :change data, quick fix as current data does not have target at end

    val listColumns = columns map (u => if (u.getName equalsIgnoreCase target) (target, targetType)
      else (u.getName, attributeTypeFromColumnClass(u.getColumnClass)))
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

}

