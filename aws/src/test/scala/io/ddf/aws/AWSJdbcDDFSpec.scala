package io.ddf.aws

import io.ddf.DDFManager
import io.ddf.jdbc.analytics.AnalyticsBehaviors
import io.ddf.jdbc.content.ContentBehaviors
import io.ddf.jdbc.etl.ETLBehaviors
import io.ddf.jdbc.{JdbcDDFManager, Loader}
import org.scalatest.FlatSpec

class AWSJdbcDDFSpec extends FlatSpec with AnalyticsBehaviors with ContentBehaviors with ETLBehaviors {
  implicit val loader = AWSLoader

  it should behave like ddfWithAddressing
  it should behave like ddfWithAggregationHandler
  it should behave like ddfWithStatisticsHandler
  it should behave like ddfWithBinningHandler

  it should behave like ddfWithMetaDataHandler
  it should behave like ddfWithPersistenceHandler
  it should behave like ddfWithSchemaHandler
  it should behave like ddfWithViewHandler

  it should behave like ddfWithBasicJoinSupport
  it should behave like ddfWithMissingDataDropSupport
  it should behave like ddfWithSqlHandler
  it should behave like ddfWithBasicTransformSupport

}

object ManagerFactory {
  val jdbcDDFManager = DDFManager.get("aws").asInstanceOf[JdbcDDFManager]
}

object AWSLoader extends Loader {
  override def engine: String = "aws"

  override def jdbcDDFManager: JdbcDDFManager = ManagerFactory.jdbcDDFManager

  override def dropTableIfExists(tableName: String) = {
    jdbcDDFManager.sql("drop table if exists " + tableName + " cascade")
  }

}
