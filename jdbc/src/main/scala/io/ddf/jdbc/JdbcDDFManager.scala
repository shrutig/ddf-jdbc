package io.ddf.jdbc


import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.ddf.content.Schema
import io.ddf.jdbc.utils.Utils
import io.ddf.misc.Config
import io.ddf.{DDF, DDFManager}
import org.h2.jdbcx.JdbcConnectionPool
import org.slf4j.LoggerFactory
import scalikejdbc.{ConnectionPool, DataSourceConnectionPool}


class JdbcDDFManager extends DDFManager {

  private final val logger = LoggerFactory.getLogger(getClass)

  override def getEngine: String = "jdbc"

  val dataSource = initializeConnectionPool(getEngine)
  val memDataSource = initializeMemConnectionPool(getEngine)

  def defaultDataSourceName = "remote"
  def memDataSourceName = "mem"

  ConnectionPool.add("remote", new DataSourceConnectionPool(dataSource))
  ConnectionPool.add("memory", new DataSourceConnectionPool(memDataSource))


  def initializeConnectionPool(engine: String) = {
    val jdbcUrl = Config.getValue(engine, "jdbcUrl")
    val jdbcUser = Config.getValue(engine, "jdbcUser")
    val jdbcPassword = Config.getValue(engine, "jdbcPassword")
    val config = new HikariConfig()
    config.setJdbcUrl(jdbcUrl)
    config.setUsername(jdbcUser)
    config.setPassword(jdbcPassword)
    new HikariDataSource(config)
  }

  def initializeMemConnectionPool(engine: String) = {
    JdbcConnectionPool.create("jdbc:h2:mem:" + engine + ";DB_CLOSE_DELAY=-1", "user", "password");
  }

  override def loadTable(fileURL: String, fieldSeparator: String): DDF = {
    null
  }

  def getColumnInfo(sampleData: Seq[Array[String]],
                    hasHeader: Boolean = false,
                    doPreferDouble: Boolean = true): Array[Schema.Column] = {

    val sampleSize: Int = sampleData.length
    mLog.info("Sample size: " + sampleSize)

    val firstRow: Array[String] = sampleData.head

    val headers: Seq[String] = if (hasHeader) {
      firstRow.toSeq
    } else {
      val size: Int = firstRow.length
      (1 to size) map (i => s"V$i")
    }

    val sampleStrings = if (hasHeader) sampleData.tail else sampleData

    val samples = sampleStrings.toArray.transpose

    samples.zipWithIndex.map {
      case (col, i) => new Schema.Column(headers(i), Utils.determineType(col, doPreferDouble, false))
    }
  }


}
