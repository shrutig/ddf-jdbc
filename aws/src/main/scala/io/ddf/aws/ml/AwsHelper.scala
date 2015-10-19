package io.ddf.aws.ml


import java.io._

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}

import scala.io.Source


class AwsHelper(s3Properties: S3Properties) {

  val s3Client = new AmazonS3Client(s3Properties.credentials)

  /*
 * We are making a modified manifest as Redshift does not like the one that AWS/ML made. Stupid but true!
 */
  def makeModifiedManifestString(batchId: String): String = {
    val obj: InputStream = s3Client.getObject(s3Properties.s3BucketName, s3Properties.s3Key + "batch-prediction/" +
      batchId + ".manifest") getObjectContent()
    val oldManifest = Source.fromInputStream(obj).mkString
    val allEntries = oldManifest.trim.stripPrefix("{").stripSuffix("}")
    val manifestEntries = allEntries.split(",").map(u => u.split("\":\"")(1)).filterNot(u => u.endsWith("tmp.gz\""))
    val modifiedManifest: String = manifestEntries.map(u => "{\"url\":" + "\"" + u + "},").mkString
    val newManifest = "{\"entries\":[" + modifiedManifest.stripSuffix(",") + "]}"
    newManifest
  }

  def createResultsManifestForRedshift(batchId: String): String = {
    val newManifest: String = makeModifiedManifestString(batchId)
    uploadStringToS3(newManifest, s3Properties.s3Key + batchId + ".manifest", s3Properties.s3BucketName)
    val url = s3Properties.s3StagingURI + batchId + ".manifest"
    url
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
    val id = s3Properties.credentials.getAWSAccessKeyId
    val key = s3Properties.credentials.getAWSSecretKey
    val region = s3Properties.s3Region
    val copySql = s"COPY $tableName from '$s3Source' credentials "
    val accessInfo = s"'aws_access_key_id=$id;aws_secret_access_key=$key'"
    s"$copySql $accessInfo manifest  delimiter ',' REGION '$region' gzip IGNOREHEADER 1 ;"
  }

  def selectSql(tableName: String) = s"SELECT * FROM $tableName"

  def truncate(value: String, length: Int): String = {
    if (value != null && value.length() > length) {
      value.substring(0, length)
    }
    else
      value
  }

}

