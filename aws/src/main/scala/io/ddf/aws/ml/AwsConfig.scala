package io.ddf.aws.ml

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.machinelearning.model.{RedshiftDatabaseCredentials, RedshiftDatabase}
import io.ddf.aws.AWSDDFManager
import io.ddf.datasource.JDBCDataSourceCredentials
import io.ddf.jdbc.content.{Representations, TableNameRepresentation}

import scala.util.Random


object AwsConfig{

  def getPropertiesForAmazonML(ddfManager: AWSDDFManager) = {
    val credentials = ddfManager.credentials
    val accessId = ddfManager.getRequiredValue("awsAccessId")
    val accessKey = ddfManager.getRequiredValue("awsAccessKey")
    val redshiftDatabaseName = ddfManager.getRequiredValue("redshiftDatabase")
    val redshiftClusterId = ddfManager.getRequiredValue("redshiftClusterId")
    val roleArn = ddfManager.getRequiredValue("redshiftIAMRoleARN")
    val s3StagingBucket = ddfManager.getRequiredValue("s3StagingBucket")
    val s3Region = ddfManager.getRequiredValue("s3Region")
    val bucketName = ddfManager.getRequiredValue("bucketName")
    val key = ddfManager.getRequiredValue("key")

    val awsCredentials = new BasicAWSCredentials(accessId, accessKey)
    val redshiftDatabase = new RedshiftDatabase().withDatabaseName(redshiftDatabaseName).withClusterIdentifier(redshiftClusterId)
    val redshiftDatabaseCredentials = new RedshiftDatabaseCredentials().withUsername(credentials.getUsername).withPassword(credentials.getPassword)
    val s3Properties = S3Properties(awsCredentials, s3StagingBucket, s3Region,bucketName,key)
    AwsProperties(awsCredentials, redshiftDatabase, redshiftDatabaseCredentials, credentials, s3Properties, roleArn)
  }
}

object Identifiers {
  private val BASE62_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray

  private def base62RandomChar(): Char = BASE62_CHARS(Random.nextInt(BASE62_CHARS.length))

  private def generateEntityId(prefix: String): String = {
    val rand = List.fill(11)(base62RandomChar()).mkString
    s"${prefix}_$rand"
  }

  def newDataSourceId: String = generateEntityId("ds")

  def newMLModelId: String = generateEntityId("ml")

  def newEvaluationId: String = generateEntityId("ev")

  def newBatchPredictionId: String = generateEntityId("bp")

  def newTableName(name: String): String = generateEntityId(name)

  def newManifestId: String = generateEntityId("manifest")

  val representation: Array[Class[TableNameRepresentation]] = Array(Representations.VIEW)

}

case class S3Properties(credentials: BasicAWSCredentials,
                        s3StagingBucket: String,
                        s3Region: String,
                         bucketName:String,
                         key:String)

case class AwsProperties(credentials: BasicAWSCredentials,
                         redshiftDatabase: RedshiftDatabase,
                         redshiftDatabaseCredentials: RedshiftDatabaseCredentials,
                         jdbcCredentials: JDBCDataSourceCredentials,
                         s3Properties: S3Properties,
                         mlIAMRoleARN: String)
