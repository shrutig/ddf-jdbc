package io.ddf.aws.ml

import java.sql.{PreparedStatement, Connection}

import io.ddf.DDF
import io.ddf.aws.AWSDDFManager
import io.ddf.misc.{Config}

class MLModel(rawModel: Object) extends io.ddf.ml.Model(rawModel) {

  val SQL_REGRESSION = "CREATE TABLE ? (score float8)"
  val SQL_BINARY = "CREATE TABLE ? (bestAnswer int4,score float8)"
  val SQL_ClASSIFICATION = "CREATE TABLE ? ();"

  def predict(ddf:DDF,var1:Array[Double]):Double ={
   AwsModelHelper.predict(ddf,var1,rawModel.toString)
  }

  def predictDataSource(ddf: DDF, datasourceId: String): DDF = {
    val batchId = AwsModelHelper.createBatchPrediction(rawModel.toString, datasourceId, Config.getValue(ddf.getEngine,
      "s3outputUrl"))
    val tableName = Identifiers.newTableName(rawModel.toString)
    val newDDF = rawModel.toString match {
      case "BINARY" => getDDF(SQL_BINARY, tableName, ddf)
      case "CLASSIFICATION" => getDDF(SQL_ClASSIFICATION, tableName, ddf)
      case "REGRESSION" => getDDF(SQL_REGRESSION, tableName, ddf)
    }
    AwsModelHelper.copyFromS3(ddf, AwsModelHelper.getNewManifestPath(rawModel.toString),
      Config.getValue(ddf.getEngine, "region"), tableName,true)
    newDDF
  }

  def getDDF(sql: String, table: String, ddf: DDF): DDF = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      connection = ddf.getManager.asInstanceOf[AWSDDFManager].getConnection
      preparedStatement = connection.prepareStatement(sql)
      preparedStatement.setString(1, table)
      val stmt = preparedStatement.toString
      ddf.getManager.asInstanceOf[AWSDDFManager].create(stmt)
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
}