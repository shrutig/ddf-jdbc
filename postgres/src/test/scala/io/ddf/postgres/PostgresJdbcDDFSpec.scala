package io.ddf.postgres

import io.ddf.DDFManager
import io.ddf.jdbc.analytics.AnalyticsBehaviors
import io.ddf.jdbc.content.ContentBehaviors
import io.ddf.jdbc.etl.ETLBehaviors
import io.ddf.jdbc.{EngineDescriptor, JdbcDDFManager, Loader}
import org.scalatest.FlatSpec

class PostgresJdbcDDFSpec extends FlatSpec with AnalyticsBehaviors with ContentBehaviors with ETLBehaviors {
  implicit val loader = PostgresLoader

  it should behave like ddfWithAddressing
  it should behave like ddfWithAggregationHandler
  it should behave like ddfWithStatisticsHandler
  it should behave like ddfWithBinningHandler

  it should behave like ddfWithMetaDataHandler
  it should behave like ddfWithPersistenceHandler
  it should behave like ddfWithSchemaHandler
  it should behave like ddfWithViewHandler

  it should behave like ddfWithBasicJoinSupport
  //it should behave like ddfWithSemiJoinSupport - unsupported
  it should behave like ddfWithFullOuterJoinSupport
  it should behave like ddfWithRightOuterJoinSupport
  it should behave like ddfWithMissingDataFillSupport
  it should behave like ddfWithMissingDataDropSupport
  it should behave like ddfWithSqlHandler
  it should behave like ddfWithBasicTransformSupport


}

object ManagerFactory {
  val engineDescriptor = EngineDescriptor("postgres")
  val jdbcDDFManager = DDFManager.get("postgres",engineDescriptor).asInstanceOf[JdbcDDFManager]
}

object PostgresLoader extends Loader {
  override def engine: String = "postgres"

  override def jdbcDDFManager: JdbcDDFManager = ManagerFactory.jdbcDDFManager

  override def dropTableIfExists(tableName: String) = {
    jdbcDDFManager.drop("drop table if exists " + tableName + " cascade")
  }

  override def MT_CARS_CREATE = "CREATE TABLE mtcars (mpg decimal,cyl int, disp decimal, hp int, drat decimal, wt decimal, qsec decimal, vs int, am int, gear int, carb int)"
}
