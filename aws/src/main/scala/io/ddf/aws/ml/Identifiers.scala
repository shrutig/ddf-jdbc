package io.ddf.aws.ml

import io.ddf.jdbc.content.{TableNameRepresentation, Representations}

import scala.util.Random

object Identifiers {
  private val BASE62_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray

  private def base62RandomChar(): Char = BASE62_CHARS(Random.nextInt(BASE62_CHARS.length))

  private def generateEntityId(prefix: String): String = {
    val rand = List.fill(11)(base62RandomChar()).mkString
    s"$prefix-$rand"
  }
  def representation():Array[Class[TableNameRepresentation]]={
    Array(Representations.VIEW)
  }
  def newDataSourceId: String = generateEntityId("ds")

  def newMLModelId: String = generateEntityId("ml")

  def newEvaluationId: String = generateEntityId("ev")

  def newBatchPredictionId: String = generateEntityId("bp")

  def newTableName(model :String) :String = generateEntityId(model)
}
/*
"org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-mllib" % "1.5.0",
  "org.apache.spark" %% "spark-sql" % "1.5.0",
  "org.apache.spark" %% "spark-hive" % "1.5.0",
 */