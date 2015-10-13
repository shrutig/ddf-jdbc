package io.ddf.aws.ml

import java.sql.{PreparedStatement, Connection}

import io.ddf.DDF
import io.ddf.aws.AWSDDFManager
import io.ddf.misc.{Config}

class MLModel(rawModel: Object) extends io.ddf.ml.Model(rawModel) {

  val SQL_REGRESSION = "CREATE TABLE ? (score float8)"
  val SQL_BINARY = "CREATE TABLE ? (bestAnswer int4,score float8)"
  //TODO:Decide to include or not as columns cannot be made directly into table
  val SQL_ClASSIFICATION = "CREATE TABLE ? ();"

  def predict(ddf: DDF, var1: Array[Double]): Double = {
    AwsModelHelper.predict(ddf, var1, rawModel.asInstanceOf[List[String]](0))
  }

  def predictDataSource(ddf: DDF, datasourceId: String): DDF = {
    val batchId = AwsModelHelper.createBatchPrediction(rawModel.asInstanceOf[List[String]](0), datasourceId,
      Config.getValue(ddf.getEngine, "s3outputUrl"))
    val tableName = Identifiers.newTableName(rawModel.asInstanceOf[List[String]](0))
    val newDDF = rawModel.asInstanceOf[List[String]](1) match {
      case "BINARY" => getDDF(s"CREATE TABLE $tableName (bestAnswer int4,score float8)", ddf)
      case "CLASSIFICATION" => getDDF(s"CREATE TABLE ? (score float8)",  ddf)
      case "REGRESSION" => getDDF(s"CREATE TABLE $tableName (score float8)", ddf)
    }
    AwsModelHelper.copyFromS3(ddf, AwsModelHelper.getNewManifestPath(rawModel.asInstanceOf[List[String]](0)),
      Config.getValue(ddf.getEngine, "region"), tableName, true)
    newDDF
  }

  def getDDF(sql: String, ddf: DDF): DDF = {
    ddf.getManager.asInstanceOf[AWSDDFManager].create(sql)

  }
}