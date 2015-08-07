package io.ddf.jdbc


import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.ddf.content.Schema
import io.ddf.jdbc.content.{Load, LoadCommand, SchemaToCreate}
import io.ddf.jdbc.utils.Utils
import io.ddf.misc.Config
import io.ddf.{DDF, DDFManager}
import scalikejdbc.{ConnectionPool, DataSourceConnectionPool}


class JdbcDDFManager extends DDFManager {

  override def getEngine: String = "jdbc"

  val dataSource = initializeConnectionPool(getEngine)

  def defaultDataSourceName = "remote"

  def baseSchema = Config.getValue(getEngine, "baseSchema")

  ConnectionPool.add("remote", new DataSourceConnectionPool(dataSource))


  def initializeConnectionPool(engine: String) = {
    val jdbcUrl = Config.getValue(engine, "jdbcUrl")
    val jdbcUser = Config.getValue(engine, "jdbcUser")
    val jdbcPassword = Config.getValue(engine, "jdbcPassword")
    val poolSizeStr = Config.getValue(engine, "jdbcPoolSize")
    val driverClassName = Config.getValue(engine, "jdbcDriverClass")
    val connectionTestQuery = Config.getValue(engine, "jdbcConnectionTestQuery")

    val poolSize = if (poolSizeStr == null) 10 else poolSizeStr.toInt

    val config = new HikariConfig()
    if (driverClassName != null) config.setDriverClassName(driverClassName)
    if (connectionTestQuery != null) config.setConnectionTestQuery(connectionTestQuery)
    config.setJdbcUrl(jdbcUrl)
    config.setUsername(jdbcUser)
    config.setPassword(jdbcPassword)
    config.setMaximumPoolSize(poolSize)
    new HikariDataSource(config)
  }


  override def loadTable(fileURL: String, fieldSeparator: String): DDF = {
    val tableName = getDummyDDF.getSchemaHandler.newTableName()
    val load = new Load(tableName, fieldSeparator.charAt(0), fileURL, null, null, true)
    val lines = LoadCommand.getLines(load, 5)
    import scala.collection.JavaConverters._
    val colInfo = getColumnInfo(lines.asScala.toList, false, true)
    val schema = new Schema(tableName, colInfo)
    val createCommand = SchemaToCreate(defaultDataSourceName, schema)
    val ddf = sql2ddf(createCommand)
    LoadCommand(this, defaultDataSourceName, load)
    ddf
  }

  def getColumnInfo(sampleData: List[Array[String]],
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
