package io.ddf.jdbc

import io.ddf.DDFManager
import io.ddf.DDFManager.EngineType
import io.ddf.jdbc.analytics.AnalyticsBehaviors
import io.ddf.jdbc.content.ContentBehaviors
import io.ddf.jdbc.etl.ETLBehaviors
import org.scalatest.FlatSpec

class H2JdbcDDFSpec extends FlatSpec with AnalyticsBehaviors with ContentBehaviors with ETLBehaviors {
  implicit val loader = H2Loader
  it should behave like ddfWithAddressing
  it should behave like ddfWithAggregationHandler
  it should behave like ddfWithStatisticsHandler
  it should behave like ddfWithBinningHandler

  it should behave like ddfWithMetaDataHandler
  it should behave like ddfWithPersistenceHandler
  it should behave like ddfWithSchemaHandler
  it should behave like ddfWithViewHandler

  it should behave like ddfWithBasicJoinSupport
  //  it should behave like ddfWithSemiJoinSupport - unsupported
  //  it should behave like ddfWithFullOuterJoinSupport - unsupported
  //  it should behave like ddfWithRightOuterJoinSupport - unsupported
  //  it should behave like ddfWithMissingDataFillSupport - unsupported

  it should behave like ddfWithMissingDataDropSupport
  it should behave like ddfWithSqlHandler
  it should behave like ddfWithBasicTransformSupport

}

object ManagerFactory {
  val engineDescriptor = EngineDescriptor(EngineType.JDBC.toString)
  val jdbcDDFManager = DDFManager.get(EngineType.JDBC, engineDescriptor).asInstanceOf[JdbcDDFManager]
}

object H2Loader extends Loader {
  override def engine: EngineType = EngineType.JDBC

  override def jdbcDDFManager: JdbcDDFManager = ManagerFactory.jdbcDDFManager
}


