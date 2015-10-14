package io.ddf.aws.ml

import java.sql.{PreparedStatement, Connection}

import io.ddf.DDF
import io.ddf.aws.AWSDDFManager
import io.ddf.exception.DDFException
import io.ddf.misc.{Config}

class MLModel(rawModel: Object) extends io.ddf.ml.Model(rawModel) {

  val SQL_REGRESSION = "CREATE TABLE ? (score float8)"
  val SQL_BINARY = "CREATE TABLE ? (bestAnswer int4,score float8)"
  //TODO:Decide to include or not as columns cannot be made directly into table
  val SQL_ClASSIFICATION = "CREATE TABLE ? ();"

  def predict(ddf: DDF, var1: Array[Double]): Double = {
    AwsModelHelper.predict(ddf, var1, getModelId)
  }

  def getModelId ={
    rawModel.asInstanceOf[Seq[String]](0)
  }

  def getModelType ={
    rawModel.asInstanceOf[Seq[String]](1)
  }

  def predictDataSource(ddf: DDF, datasourceId: String): DDF = {
    val batchId = AwsModelHelper.createBatchPrediction(getModelId, datasourceId,
      Config.getValue(ddf.getEngine, "s3outputUrl"))
    val tableName = Identifiers.newTableName(getModelId)
    val newDDF = getModelType match {
     /* case "BINARY" => getDDF(s"CREATE TABLE $tableName (bestAnswer int4,score float8)", ddf)*/ //only temporaray
      case "BINARY" => getDDF(s"CREATE TABLE  $tableName (score float8)",  ddf)
      case "MULTICLASS" => getDDF(s"CREATE TABLE ? (score float8)",  ddf)
      case "REGRESSION" => getDDF(s"CREATE TABLE $tableName (score float8)", ddf)
      case whatever => throw new DDFException("Wrong model name called . Please use one of BINARY,MULTICLASS " +
        "orREGRESSION ")
    }
    AwsModelHelper.copyFromS3(ddf, AwsModelHelper.getNewManifestPath(rawModel.asInstanceOf[Seq[String]](0)),
      Config.getValue(ddf.getEngine, "region"), tableName, true)
    newDDF
  }

  def getDDF(sql: String, ddf: DDF): DDF = {
    ddf.getManager.asInstanceOf[AWSDDFManager].create(sql)

  }
}