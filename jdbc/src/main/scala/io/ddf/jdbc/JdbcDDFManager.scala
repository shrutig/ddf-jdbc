package io.ddf.jdbc


import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.ddf.content.Schema
import io.ddf.jdbc.utils.Utils
import io.ddf.misc.Config
import io.ddf.{DDF, DDFManager}
import org.slf4j.LoggerFactory
import scalikejdbc.{ConnectionPool, DataSourceConnectionPool}


class JdbcDDFManager extends DDFManager {

  private final val logger = LoggerFactory.getLogger(getClass)

  override def getEngine: String = "jdbc"

  val dataSource = initializeConnectionPool(getEngine)

  def defaultDataSourceName = "remote"

  ConnectionPool.add("remote",new DataSourceConnectionPool(dataSource))


  def initializeConnectionPool(engine: String) = {
    val jdbcUrl = Config.getValue(engine, "jdbcUrl")
    val jdbcUser = Config.getValue(engine, "jdbcUser")
    val jdbcPassword = Config.getValue(engine, "jdbcPassword")
    val poolSizeStr= Config.getValue(engine, "jdbcPoolSize")
    val poolSize = if(poolSizeStr==null) 10 else poolSizeStr.toInt

    val config = new HikariConfig()
    config.setJdbcUrl(jdbcUrl)
    config.setUsername(jdbcUser)
    config.setPassword(jdbcPassword)
    config.setMaximumPoolSize(poolSize)
    new HikariDataSource(config)
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
