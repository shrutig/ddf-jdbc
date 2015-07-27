package io.ddf.jdbc

import io.ddf.{DDF, DDFManager}
import org.scalatest.{FlatSpec, Matchers}

class BaseSpec extends FlatSpec with Matchers {
  val jdbcDDFManager = DDFManager.get("jdbc").asInstanceOf[JdbcDDFManager]
  var lDdf: DDF = null

  def ddf = loadDDF()

  def loadDDF(): DDF = {
    if (lDdf == null)
      lDdf = jdbcDDFManager.loadTable(getClass.getResource("/airline.csv").getPath, ",")
    lDdf
  }

  def loadIrisTrain(): DDF = {
    try {
      jdbcDDFManager.getDDFByName("iris")
    } catch {
      case e: Exception =>
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
        jdbcDDFManager.sql("create table airline (Year int,Month int,DayofMonth int," + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," + "CRSArrTime int,UniqueCarrier string, FlightNum int, " + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " + "AirTime int, ArrDelay int, DepDelay int, Origin string, " + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " + "CancellationCode string, Diverted string, CarrierDelay int, " + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )")
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
        jdbcDDFManager.sql("create table airlineWithNA (Year int,Month int,DayofMonth int," + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," + "CRSArrTime int,UniqueCarrier string, FlightNum int, " + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " + "AirTime int, ArrDelay int, DepDelay int, Origin string, " + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " + "CancellationCode string, Diverted string, CarrierDelay int, " + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )")
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
        jdbcDDFManager.sql("create table year_names (Year_num int,Name string)")
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
