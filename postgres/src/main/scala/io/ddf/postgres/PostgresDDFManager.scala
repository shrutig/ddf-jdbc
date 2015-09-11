package io.ddf.postgres

import java.sql.{Connection, DatabaseMetaData, ResultSet}
import java.util

import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.content.Schema.ColumnType
import io.ddf.datasource.DataSourceDescriptor
import io.ddf.jdbc.JdbcDDFManager
import io.ddf.jdbc.content.{Catalog, SqlArrayResultCommand}
import io.ddf.jdbc.utils.Utils
import scalikejdbc.{DB, SQL}

class PostgresDDFManager(dataSourceDescriptor: DataSourceDescriptor, engineName: String) extends JdbcDDFManager(dataSourceDescriptor, engineName) {
  override def getEngine = "postgres"

  override def catalog = PostgresCatalog

  catalog.curSchema = this.baseSchema

  override def setSchema(schemaName: String): Unit = {
    this.sql("set search_path to " + schemaName)
  }

}


object PostgresCatalog extends Catalog {

  var curSchema : String = "public"

  def log(str: String): Unit = {
    this.mLog.info(str)
  }

  override def getViewSchema(connection: Connection, schemaName: String, name: String): Schema = getTableSchema(connection, schemaName, name)

  override def getTableSchema(connection: Connection, schemaName: String, name: String): Schema = {
    implicit val catalog = this
    val sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '" + schemaName.toLowerCase + "' AND table_name ='" + name.toLowerCase + "' order by ordinal_position"
    val columns: util.List[Column] = new util.ArrayList[Column]
    val sqlResult = SqlArrayResultCommand(connection, "information_schema", name, sql)
    sqlResult.result.foreach { row =>
      val columnName = row(0).toString
      var columnTypeStr = row(1).toString

      /*
      if (columnTypeStr.equalsIgnoreCase("character varying") || "text".equalsIgnoreCase(columnTypeStr) || "VARCHAR".equalsIgnoreCase(columnTypeStr) || "VARCHAR2".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "STRING"
      }
      if ("double precision".equalsIgnoreCase(columnTypeStr) || "decimal".equalsIgnoreCase(columnTypeStr) || "real".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "DOUBLE"
      }
      if ("numeric".equalsIgnoreCase(columnTypeStr) || "serial".equalsIgnoreCase(columnTypeStr) || "smallint".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "INTEGER"
      }
      if ("bigserial".equalsIgnoreCase(columnTypeStr) || "bigint".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "BIGINT"
      }
      */
      this.mLog.info("list columns: " + columnName + " " + columnTypeStr)
      val column = new Column(columnName, this.getColumnType(columnTypeStr))
      columns.add(column)
    }
    new Schema(name, columns)
  }


  override def showTables(connection: Connection, schemaName: String): util.List[String] = {
      var schema: String = schemaName
      if (schema == null) {
        schema = curSchema
      }
      val sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema.toLowerCase + "'"
    val tables: util.List[String] = new util.ArrayList[String]
    implicit val catalog = this
    val sqlResult = SqlArrayResultCommand(connection, "information_schema", "tables", sql)
    sqlResult.result.foreach(row => tables.add(row(0).toString))
    tables
  }

  override def showDatabases(connection: Connection): util.List[String] = {
    val sql = "SELECT datname FROM pg_database WHERE datistemplate = false;";
    val databases: util.List[String] = new util.ArrayList[String]
    implicit  val catalog = this
    val sqlResult = SqlArrayResultCommand(connection, "pg_database",
      "tables", sql)
    sqlResult.result.foreach(row => databases.add(row(0).toString))
    databases
  }

  override def setDatabase(connection: Connection, database: String): Unit = {
    connection.setCatalog(database)
  }

  override  def listColumnsForTable(connection: Connection, schemaName: String,
                                    tableName: String): util.List[Column] = {
    val columns: util.List[Column] = new util.ArrayList[Column]
    val metadata: DatabaseMetaData = connection.getMetaData
    val resultSet: ResultSet = metadata.getColumns(null, schemaName, tableName, null)
    while (resultSet.next) {
      val columnName = resultSet.getString(4)
      var columnType = resultSet.getInt(5)
      this.mLog.info("list columns: " + columnName + " " + columnType);
      val column = new Column(columnName, Utils.getDDFType(columnType))
      columns.add(column)
    }
    columns
  }

  override def showSchemas(connection: Connection): util.List[String] = {
    val rs = connection.getMetaData.getSchemas()
    val schemas: util.List[String] = new util.ArrayList[String]()
    while (rs.next()) {
      schemas.add(rs.getString("TABLE_SCHEM"))
    }
    schemas
  }

  override def setSchema(connection: Connection, schemaName: String): Unit = {
      implicit val session = DB(connection).autoCommitSession()
      SQL("set search_path to " + schemaName).execute()
  }


  override def getColumnType(typeStr: String) : ColumnType = {
    typeStr match {
      case "int4" => ColumnType.INT
      case "integer" => ColumnType.INT
      case "int8"=> ColumnType.BIGINT
      case "float4"=> ColumnType.FLOAT
      case "float8" => ColumnType.DOUBLE
      case "varchar"=> ColumnType.STRING
      case "varchar2"=> ColumnType.STRING
      case "name"=> ColumnType.STRING
      case "text"=> ColumnType.STRING
      case "date"=> ColumnType.DATE
      case "bool"=> ColumnType.BOOLEAN
    }
  }

}
