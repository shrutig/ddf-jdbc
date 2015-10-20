package io.ddf.aws.ml

import java.util.Date
import java.util.concurrent.TimeUnit

import com.amazonaws.services.machinelearning.AmazonMachineLearningClient
import com.amazonaws.services.machinelearning.model._
import io.ddf.content.Schema
import io.ddf.content.Schema.ColumnClass
import io.ddf.exception.DDFException

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

class AwsMLHelper(awsProperties: AwsProperties) {
  val client = new AmazonMachineLearningClient(awsProperties.credentials)

  def createModel(trainDataSourceId: String, modelType: MLModelType, parameters: java.util.Map[String,
    String]): String = {
    val modelId = Identifiers.newMLModelId
    val request = new CreateMLModelRequest()
      .withMLModelId(modelId)
      .withMLModelName(modelId)
      .withMLModelType(modelType)
      .withTrainingDataSourceId(trainDataSourceId)
    if (!parameters.isEmpty) {
      request.withParameters(parameters)
    }
    client.createMLModel(request)
    //wait for this to complete
    waitForModel(modelId)
    modelId
  }


  def createDataSourceFromRedShift(schema: Schema, sqlQuery: String, mLModelType: MLModelType): String = {
    val dataSourceId = Identifiers.newDataSourceId
    val dataSpec = new RedshiftDataSpec()
      .withDatabaseInformation(awsProperties.redshiftDatabase)
      .withDatabaseCredentials(awsProperties.redshiftDatabaseCredentials)
      .withSelectSqlQuery(sqlQuery)
      .withS3StagingLocation(awsProperties.s3Properties.s3StagingURI)
      .withDataSchema(getSchemaAttributeDataSource(schema, mLModelType))
    val request = new CreateDataSourceFromRedshiftRequest()
      .withComputeStatistics(true)
      .withDataSourceId(dataSourceId)
      .withDataSpec(dataSpec)
      .withRoleARN(awsProperties.mlIAMRoleARN)
    client.createDataSourceFromRedshift(request)
    dataSourceId
  }

  def getSchemaAttributeDataSource(schema: Schema, mLModelType: MLModelType): String = {
    val columns = schema.getColumns.asScala
    //for supervised learning last column is the target column
    val target = columns.last.getName
    val targetType = getTargetAttributeType(mLModelType)
    val listColumns = columns map (u => (u.getName, attributeTypeFromColumnClass(u.getColumnClass)))
    val attributes = listColumns.map { case (name, attrType) =>
      val actualType = if (name.equals(target)) targetType else attrType
      "{\"attributeName\":" + "\"" + name + "\",\"attributeType\": \"" + actualType + "\"\n}"
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

  def createTableSqlForModelType(mLModelType: MLModelType, tableName: String, uniqueTargetVal: String): String = {
    mLModelType match {
      case MLModelType.BINARY => s"CREATE TABLE $tableName (trueLabel int4, bestAnswer int4,score float8)"
      case MLModelType.REGRESSION => s"CREATE TABLE $tableName (trueLabel float8, score float8)"
      case MLModelType.MULTICLASS => s"CREATE TABLE $tableName (trueLabel varchar, $uniqueTargetVal)"
    }
  }

  def getTargetAttributeType(mLModelType: MLModelType): String = {
    mLModelType match {
      case MLModelType.BINARY => "BINARY"
      case MLModelType.REGRESSION => "NUMERIC"
      case MLModelType.MULTICLASS => "CATEGORICAL"
      case _ => throw new IllegalArgumentException
    }
  }


  def createBatchPrediction(awsModel: AwsModel, dataSourceId: String): String = {
    val batchPredictionId = Identifiers.newBatchPredictionId
    val bpRequest = new CreateBatchPredictionRequest()
      .withBatchPredictionId(batchPredictionId)
      .withBatchPredictionName("Batch Prediction for " + dataSourceId)
      .withMLModelId(awsModel.getModelId)
      .withOutputUri(awsProperties.s3Properties.s3StagingURI)
      .withBatchPredictionDataSourceId(dataSourceId)
    client.createBatchPrediction(bpRequest)
    //wait for this to complete
    waitFoBatchPrediction(batchPredictionId)
    batchPredictionId
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


  def waitForCompletion(entityId: String, entityType: String, delay: FiniteDuration = FiniteDuration(60, TimeUnit
    .SECONDS)): Unit = {
    var terminated = false
    def getStatus = {
      entityType match {
        case "MODEL" => val request = new GetMLModelRequest().withMLModelId(entityId)
          client.getMLModel(request).getStatus
        case "EVALUATION" => val request = new GetEvaluationRequest().withEvaluationId(entityId)
          client.getEvaluation(request).getStatus
        case "BATCH_PREDICTION" => val request = new GetBatchPredictionRequest().withBatchPredictionId(entityId)
          client.getBatchPrediction(request).getStatus
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
    (answer get parameter).toDouble
  }

  def predict(inputs: Seq[_], awsModel: AwsModel): Either[Float, String] = {
    val modelId = awsModel.getModelId
    val prediction = new PredictRequest()
    val predictEndPoint = client.createRealtimeEndpoint(new CreateRealtimeEndpointRequest().withMLModelId(modelId))
    val columns = awsModel.getSchema.getColumnNames.asScala
    val pair = columns zip inputs
    pair foreach { case (colName, value) =>
      prediction.addRecordEntry(colName, value.toString)
    }
    prediction.setMLModelId(modelId)
    prediction.setPredictEndpoint(predictEndPoint.getRealtimeEndpointInfo.getEndpointUrl)
    val predictResult = client.predict(prediction)
    awsModel.getMLModelType match {
      case MLModelType.BINARY => Right(predictResult.getPrediction.getPredictedLabel)
      case MLModelType.MULTICLASS => Right(predictResult.getPrediction.getPredictedLabel)
      case MLModelType.REGRESSION => Left(predictResult.getPrediction.getPredictedValue)
    }
  }

}
