package io.ddf.teradata

import java.util.UUID

import io.ddf.DDFManager
import io.ddf.DDFManager.EngineType
import io.ddf.exception.DDFException
import io.ddf.jdbc.analytics.AnalyticsBehaviors
import io.ddf.jdbc.content._
import io.ddf.jdbc.etl.ETLBehaviors
import io.ddf.jdbc.{EngineDescriptor, JdbcDDFManager, Loader}
import io.ddf.misc.ALoggable
import io.ddf.teradata.TeradataDDFManager
import org.scalatest.FlatSpec
import io.ddf.misc.Config

class TeradataJDBCSpec extends FlatSpec with TeradataAnalyticsBehaviors
with TeradataContentBehaviors with TeradataETLBehaviors {

  implicit val loader = TeradataLoader
  it should behave like ddfWithAddressing
  it should behave like ddfWithAggregationHandler
  it should behave like ddfWithStatisticsHandler
  it should behave like ddfWithBinningHandler

  it should behave like ddfWithMetaDataHandler
  it should behave like ddfWithPersistenceHandler
  it should behave like ddfWithSchemaHandler
  it should behave like ddfWithViewHandler

  it should behave like ddfWithBasicJoinSupport
  //it should behave like ddfWithSemiJoinSupport //- unsupported
  it should behave like ddfWithFullOuterJoinSupport
  it should behave like ddfWithRightOuterJoinSupport
  it should behave like ddfWithMissingDataFillSupport

  it should behave like ddfWithMissingDataDropSupport
  it should behave like ddfWithSqlHandler
  it should behave like ddfWithBasicTransformSupport

}

object ManagerFactory {
  val engineDescriptor = EngineDescriptor("teradata")
  val jdbcDDFManager = DDFManager.get(EngineType.TERADATA, engineDescriptor).
    asInstanceOf[TeradataDDFManager]
}


object TeradataLoader extends Loader {

  override def engine: EngineType = EngineType.TERADATA

  override def dropTableIfExists(tableName: String) = {

    implicit def catalog: Catalog = SimpleCatalog
    val newTableName = UUID.randomUUID().toString
    // SqlCommand is used to check if the table exists.
    // If table does not exist, it returns an empty list.
    val exists = SqlCommand(
      jdbcDDFManager.getConnection(),
      baseSchema,
      newTableName,
      "SELECT 1 FROM dbc.tables WHERE " + "DatabaseName='" + jdbcDDFManager.db +
        "' AND TableName='" + tableName + "' AND TableKind='T'",
      1,
      "\t",
      jdbcDDFManager.getEngine).getRows
    if (!exists.isEmpty) {
      jdbcDDFManager.drop("drop table " + jdbcDDFManager.db + "." + tableName)
      1
    }
    else
      0
  }

  override def AIRLINE_CREATE = """create table airline (Year1 int, Month1 int,
                                  DayofMonth int, DayOfWeek int,DepTime int,
                                  CRSDepTime int, ArrTime int, CRSArrTime int,
                                  UniqueCarrier varchar(2), FlightNum int,
                                  TailNum varchar(7), ActualElapsedTime int,
                                  CRSElapsedTime int, AirTime int, ArrDelay int,
                                  DepDelay int, Origin varchar(3), Dest
                                  varchar(3), Distance int, TaxiIn int, TaxiOut
                                  int, Cancelled int, CancellationCode varchar(2),
                                  Diverted varchar(2), CarrierDelay int,
                                  WeatherDelay int, NASDelay int, SecurityDelay
                                  int, LateAircraftDelay int)"""

  override def AIRLINE_NA_CREATE = """create table airlineWithNA (Year1 int,
                                    Month1 int,DayofMonth int, DayOfWeek int,
                                    DepTime int,CRSDepTime int, ArrTime int,
                                    CRSArrTime int,UniqueCarrier varchar(2),
                                    FlightNum int, TailNum varchar(7),
                                    ActualElapsedTime int, CRSElapsedTime int,
                                    AirTime int, ArrDelay int, DepDelay int,
                                    Origin varchar(3), Dest varchar(3),
                                    Distance int,TaxiIn int, TaxiOut int,
                                    Cancelled int, CancellationCode varchar(2),
                                    Diverted varchar(2), CarrierDelay int,
                                    WeatherDelay int, NASDelay int,
                                    SecurityDelay int, LateAircraftDelay int)"""

  override def YEAR_NAMES_CREATE =
    """create table year_names (Year_num int,Name varchar(8))"""

  override def MT_CARS_CREATE = """CREATE TABLE mtcars (mpg double precision,cyl
                                 int, disp double precision, hp int, drat double
                                 precision, wt double precision, qsec double
                                 precision, vs int, am int, gear int, carb int)"""

  var counter = true

  override def jdbcDDFManager: TeradataDDFManager = {
    val manager = ManagerFactory.jdbcDDFManager
    manager
  }
}
