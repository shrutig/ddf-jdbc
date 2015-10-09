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
  val SQL_COPY_MANIFEST = "COPY ? from ? region ? credentials " +
    " \'aws_access_key_id=?;aws_secret_access_key=?\' manifest delimiter ',' ;"

  def copyFromS3(ddf: DDF, s3Source: String, region: String, tableName: String,isManifest:Boolean) = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      connection = ddf.getManager.asInstanceOf[AWSDDFManager].getConnection
      val sql = isManifest match {
        case true => SQL_COPY_MANIFEST
        case false => SQL_COPY
      }
      preparedStatement = connection.prepareStatement(sql)
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

  def getNewManifestPath(batchId: String):String = {
    val oldManifest = getObject(batchId)
    val modifiedManifest:String = oldManifest.trim.stripPrefix("{").stripSuffix("}").split(",") map(u => u
      .split(":")(1)) map (u => "{\"url\":"+"\""+u+"\"},")  mkString("")
    val newManifest = "{\"entries\":[" + modifiedManifest  .stripSuffix(",") + "]}"
    val fileName = Identifiers.newManifestId
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(newManifest)
    bw.close()
    uploadToS3(fileName,Config.getValue(AWS, "key")+fileName+".manifest",Config.getValue(AWS,
      "bucketName"))
    Config.getValue(AWS, "key")+fileName+".manifest"
  }

  lazy val s3Client = new AmazonS3Client(credentials)

  def uploadToS3(filePath:String , fileId: String,bucketName:String) {
    val s3Client = new AmazonS3Client(credentials)
    val  file = new File(filePath)
    val objectRequest = new PutObjectRequest(bucketName, fileId, file)
    s3Client.putObject(objectRequest)
  }

  def getObject(batchId: String): String = {
    val obj: InputStream = s3Client.getObject(Config.getValue(AWS, "bucketName"), Config.getValue(AWS, "key")
      + "/batch-prediction/result" + batchId + ".manifest") getObjectContent()
    val str = Source.fromInputStream(obj).mkString
    obj.close()
    str
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

