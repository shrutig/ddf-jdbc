package io.ddf.jdbc

import io.ddf.{DDF, DDFManager}
import org.scalatest.Matchers

trait BaseBehaviors extends Matchers


trait Loader {
  def engine:String
  def jdbcDDFManager:JdbcDDFManager
  def baseSchema = jdbcDDFManager.baseSchema

  def dropTableIfExists(tableName:String) = {
    jdbcDDFManager.sql("drop table if exists " + tableName)
  }

  def loadIrisTrain(): DDF = {
    try {
      jdbcDDFManager.getDDFByName("iris")
    } catch {
      case e: Exception =>
        dropTableIfExists("iris")
        jdbcDDFManager.sql("create table iris (flower double, petal double, septal double)")
        val filePath = getClass.getResource("/fisheriris.csv").getPath
        jdbcDDFManager.sql("load '" + filePath + "' into iris")
        jdbcDDFManager.getDDFByName("iris")
    }
  }

  def loadIrisTest(): DDF = {
    val train = jdbcDDFManager.getDDFByName("iris")
    //train.sql2ddf("SELECT petal, septal FROM iris WHERE flower = 1.0000")
    train.VIEWS.project("petal", "septal")
  }

  def loadAirlineDDF(): DDF = {
    var ddf: DDF = null
    try {
      ddf = jdbcDDFManager.getDDFByName("airline")
    } catch {
      case e: Exception =>
        dropTableIfExists("airline")
        jdbcDDFManager.sql("create table airline (Year int,Month int,DayofMonth int," + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," + "CRSArrTime int,UniqueCarrier varchar, FlightNum int, " + "TailNum varchar, ActualElapsedTime int, CRSElapsedTime int, " + "AirTime int, ArrDelay int, DepDelay int, Origin varchar, " + "Dest varchar, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " + "CancellationCode varchar, Diverted varchar, CarrierDelay int, " + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )")
        val filePath = getClass.getResource("/airline.csv").getPath
        jdbcDDFManager.sql("load '" + filePath + "' into airline")
        ddf = jdbcDDFManager.getDDFByName("airline")
    }
    ddf
  }

  def loadAirlineNADDF(): DDF = {
    var ddf: DDF = null
    try {
      ddf = jdbcDDFManager.getDDFByName("airlineWithNA")
    } catch {
      case e: Exception =>
        dropTableIfExists("airlineWithNA")
        jdbcDDFManager.sql("create table airlineWithNA (Year int,Month int,DayofMonth int," + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," + "CRSArrTime int,UniqueCarrier varchar, FlightNum int, " + "TailNum varchar, ActualElapsedTime int, CRSElapsedTime int, " + "AirTime int, ArrDelay int, DepDelay int, Origin varchar, " + "Dest varchar, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " + "CancellationCode varchar, Diverted varchar, CarrierDelay int, " + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )")
        val filePath = getClass.getResource("/airlineWithNA.csv").getPath
        jdbcDDFManager.sql("load '" + filePath + "' WITH NULL '' NO DEFAULTS into airlineWithNA")
        ddf = jdbcDDFManager.getDDFByName("airlineWithNA")
    }
    ddf
  }


  def loadYearNamesDDF(): DDF = {
    var ddf: DDF = null
    try {
      ddf = jdbcDDFManager.getDDFByName("year_names")
    } catch {
      case e: Exception =>
        dropTableIfExists("year_names")
        jdbcDDFManager.sql("create table year_names (Year_num int,Name varchar)")
        val filePath = getClass.getResource("/year_names.csv").getPath
        jdbcDDFManager.sql("load '" + filePath + "' into year_names")
        ddf = jdbcDDFManager.getDDFByName("year_names")
    }
    ddf
  }

  def loadMtCarsDDF(): DDF = {
    var ddf: DDF = null
    try {
      ddf = jdbcDDFManager.getDDFByName("mtcars")
    } catch {
      case e: Exception =>
        dropTableIfExists("mtcars")
        jdbcDDFManager.sql("CREATE TABLE mtcars ("
          + "mpg double,cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
          + ")")
        val filePath = getClass.getResource("/mtcars").getPath
        jdbcDDFManager.sql("load '" + filePath + "'  delimited by ' '  into mtcars")
        ddf = jdbcDDFManager.getDDFByName("mtcars")
    }
    ddf
  }


}

