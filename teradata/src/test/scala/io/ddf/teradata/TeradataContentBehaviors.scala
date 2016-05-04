package io.ddf.teradata

import java.util

import io.ddf.DDF
import io.ddf.content.Schema.Column
import io.ddf.jdbc.Loader
import io.ddf.jdbc.content.ContentBehaviors
import org.scalatest.FlatSpec

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait TeradataContentBehaviors extends ContentBehaviors {
  this: FlatSpec =>
  /* In the following tests, year, month has been replaced by year1, month1 as in
   teradata, year and month are reserved keywords */
  override def ddfWithViewHandler(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()

    it should "project after remove columns " in {
      val ddf = airlineDDF
      val columns: java.util.List[String] = new java.util.ArrayList()
      columns.add("year1")
      columns.add("month1")
      columns.add("deptime")

      val newddf1: DDF = ddf.VIEWS.removeColumn("year1")
      val newddf2: DDF = ddf.VIEWS.removeColumns("year1", "deptime")
      val newddf3: DDF = ddf.VIEWS.removeColumns(columns)
      newddf1.getNumColumns should be(28)
      newddf2.getNumColumns should be(27)
      newddf3.getNumColumns should be(26)
    }
  }

  override  def ddfWithSchemaHandler(implicit l: Loader): Unit = {
    val manager = l.jdbcDDFManager
    val ddf = l.loadAirlineDDF()
    it should "get schema" in {
      ddf.getSchema should not be null
    }

    it should "get columns" in {
      val columns: util.List[Column] = ddf.getSchema.getColumns
      columns should not be null
      columns.length should be(29)
    }

    it should "get columns for sql2ddf create table" in {
      val ddf = l.loadAirlineDDF()
      val columns = ddf.getSchema.getColumns
      columns should not be null
      columns.length should be(29)
      columns.head.getName.toUpperCase should be("YEAR1")
    }

    it should "test get factors on DDF" in {
      val ddf = l.loadMtCarsDDF()
      val schemaHandler = ddf.getSchemaHandler
      Array(7, 8, 9, 10).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      schemaHandler.computeFactorLevelsAndLevelCounts()
      val cols = Array(7, 8, 9, 10).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }
      println("", cols.mkString(","))
      assert(cols(0).getOptionalFactor.getLevelCounts.get("1") === 14)
      assert(cols(0).getOptionalFactor.getLevelCounts.get("0") === 18)
      assert(cols(1).getOptionalFactor.getLevelCounts.get("1") === 13)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("4") === 12)

      assert(cols(2).getOptionalFactor.getLevelCounts.get("3") === 15)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("5") === 5)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("1") === 7)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("2") === 10)
    }


    it should "test get factors" in {
      val ddf = manager.sql2ddf("select * from mtcars")

      val schemaHandler = ddf.getSchemaHandler

      Array(7, 8, 9, 10).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      schemaHandler.computeFactorLevelsAndLevelCounts()

      val cols2 = Array(7, 8, 9, 10).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }

      assert(cols2(0).getOptionalFactor.getLevelCounts.get("1") === 14)
      assert(cols2(0).getOptionalFactor.getLevelCounts.get("0") === 18)
      assert(cols2(1).getOptionalFactor.getLevelCounts.get("1") === 13)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("4") === 12)

      assert(cols2(2).getOptionalFactor.getLevelCounts.get("3") === 15)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("5") === 5)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("1") === 7)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("2") === 10)
    }

    it should "test NA handling" in {
      val ddf = l.loadAirlineNADDF()
      val schemaHandler = ddf.getSchemaHandler

      Array(0, 8, 16, 17, 24, 25).foreach {
        idx => schemaHandler.setAsFactor(idx)
      }
      schemaHandler.computeFactorLevelsAndLevelCounts()

      val cols = Array(0, 8, 16, 17, 24, 25).map {
        idx => schemaHandler.getColumn(schemaHandler.getColumnName(idx))
      }
      assert(cols(0).getOptionalFactor.getLevels.contains("2008"))
      assert(cols(0).getOptionalFactor.getLevels.contains("2010"))
      assert(cols(0).getOptionalFactor.getLevelCounts.get("2008") === 28.0)
      assert(cols(0).getOptionalFactor.getLevelCounts.get("2010") === 1.0)

      assert(cols(1).getOptionalFactor.getLevelCounts.get("WN") === 28.0)

      assert(cols(2).getOptionalFactor.getLevelCounts.get("ISP") === 12.0)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("IAD") === 2.0)
      assert(cols(2).getOptionalFactor.getLevelCounts.get("IND") === 17.0)

      assert(cols(3).getOptionalFactor.getLevelCounts.get("MCO") === 3.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("TPA") === 3.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("JAX") === 1.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("LAS") === 3.0)
      assert(cols(3).getOptionalFactor.getLevelCounts.get("BWI") === 10.0)

      assert(cols(5).getOptionalFactor.getLevelCounts.get("0") === 9.0)
      assert(cols(4).getOptionalFactor.getLevelCounts.get("3") === 1.0)

      val ddf2 = manager.sql2ddf("select * from  airlineWithNA")

      val schemaHandler2 = ddf2.getSchemaHandler
      Array(0, 8, 16, 17, 24, 25).foreach {
        idx => schemaHandler2.setAsFactor(idx)
      }
      schemaHandler2.computeFactorLevelsAndLevelCounts()

      val cols2 = Array(0, 8, 16, 17, 24, 25).map {
        idx => schemaHandler2.getColumn(schemaHandler2.getColumnName(idx))
      }

      assert(cols2(0).getOptionalFactor.getLevelCounts.get("2008") === 28.0)
      assert(cols2(0).getOptionalFactor.getLevelCounts.get("2010") === 1.0)

      assert(cols2(1).getOptionalFactor.getLevelCounts.get("WN") === 28.0)

      assert(cols2(2).getOptionalFactor.getLevelCounts.get("ISP") === 12.0)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("IAD") === 2.0)
      assert(cols2(2).getOptionalFactor.getLevelCounts.get("IND") === 17.0)

      assert(cols2(3).getOptionalFactor.getLevelCounts.get("MCO") === 3.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("TPA") === 3.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("JAX") === 1.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("LAS") === 3.0)
      assert(cols2(3).getOptionalFactor.getLevelCounts.get("BWI") === 10.0)

      assert(cols2(5).getOptionalFactor.getLevelCounts.get("0") === 9.0)
      assert(cols2(4).getOptionalFactor.getLevelCounts.get("3") === 1.0)
    }
  }
}