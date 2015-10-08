package io.ddf.aws.ml

import java.io.File
import java.sql.{PreparedStatement, Connection}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.machinelearning.AmazonMachineLearningClient
import com.amazonaws.services.machinelearning.model._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.PutObjectRequest
import io.ddf.DDF
import io.ddf.aws.AWSDDFManager
import io.ddf.misc.Config

import scala.io.Source

object AwsModelHelper {
  val PREFIX = "Scala aws ml"
  val AWS = "aws"
  val credentials = new BasicAWSCredentials(Config.getValue(AWS, "accessId"),
    Config.getValue(AWS, "accessKey"))
  val client = new AmazonMachineLearningClient(credentials)
  val JDBC_NAME = "jdbcName"
  val CLUSTER_NAME = "clusterName"
  val DB_USER = "dbUser"
  val DB_PASS = "dbPass"
  val ML_STAGE = "mlStage"
  val ML_ROLE_ARN = "mlRoleArn"
  val SQL_COPY = "COPY ? from ? region ? credentials " +
    " \'aws_access_key_id=?;aws_secret_access_key=?\' delimiter ',' ;"

  def copyFromS3(ddf: DDF, s3Source: String, region: String, tableName: String) = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      connection = ddf.getManager.asInstanceOf[AWSDDFManager].getConnection
      preparedStatement = connection.prepareStatement(SQL_COPY)
      preparedStatement.setString(1, tableName)
      preparedStatement.setString(2, s3Source)
      preparedStatement.setString(3, region)
      preparedStatement.setString(4, Config.getValue(AWS, "accessId"))
      preparedStatement.setString(5, Config.getValue(AWS, "accessKey"))
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

  def createDataSourceFromRedShift(sqlQuery: String): String = {
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
      .withSelectSqlQuery(Config.getValue(AWS, sqlQuery))
      .withS3StagingLocation(Config.getValue(AWS, ML_STAGE))
    val request = new CreateDataSourceFromRedshiftRequest()
      .withComputeStatistics(false)
      .withDataSourceId(entityId)
      .withDataSpec(dataSpec)
      .withRoleARN(Config.getValue(AWS, ML_ROLE_ARN))
    client.createDataSourceFromRedshift(request)
    entityId
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
    val predicResult = client.predict(prediction)
    predicResult.getPrediction.getPredictedValue.toDouble
  }
}
