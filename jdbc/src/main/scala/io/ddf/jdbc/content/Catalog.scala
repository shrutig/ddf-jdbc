package io.ddf.jdbc.content

import io.ddf.jdbc.utils

import java.sql.{Connection, DatabaseMetaData, ResultSet, SQLException}
import java.util

import io.ddf.DDFManager
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.content.Schema.ColumnType
import io.ddf.jdbc.utils.Utils
import io.ddf.misc.ALoggable

trait Catalog extends ALoggable {

  def getViewSchema(connection: Connection, schemaName: String, tableName: String): Schema

  def getTableSchema(connection: Connection, schemaName: String, tableName: String): Schema

  def listColumnsForTable(connection: Connection,
                          schemaName: String,
                          tableName: String): util.List[Column]

  def setSchema(connection: Connection, schemaName: String)

  def showTables(connection: Connection, schemaName: String): util.List[String]

  def showViews(connection: Connection, schemaName: String): util.List[String]

  def showDatabases(connection: Connection): util.List[String]

  def setDatabase(connection: Connection, database : String)

  def showSchemas(connection: Connection): util.List[String]

  def getColumnType(typeStr: String) : ColumnType

  def log(str: String): Unit
}

object SimpleCatalog extends Catalog {
  def log(str: String): Unit = {
    this.mLog.info(str)
  }

  def getViewSchema(connection: Connection, schemaName: String, viewName: String): Schema = {
    getTableSchema(connection, schemaName, viewName)
  }

  def getTableSchema(connection: Connection, schemaName: String, tableName: String): Schema = {
    val columns = listColumnsForTable(connection, null, tableName)
    new Schema(tableName, columns)
  }

  @throws(classOf[SQLException])
  override  def listColumnsForTable(connection: Connection, schemaName: String,
                                    tableName: String): util.List[Column] = {
    var resultSet: ResultSet = null
    try {
      val columns: util.List[Column] = new util.ArrayList[Column]
      val metadata: DatabaseMetaData = connection.getMetaData
      resultSet = metadata.getColumns(null, schemaName, tableName.toUpperCase, null)
      while (resultSet.next) {
        val columnName = resultSet.getString(4)
        var columnType = resultSet.getInt(5)

        val column = new Column(columnName, Utils.getDDFType(columnType))
        columns.add(column)
      }
      columns
    }
    finally {
      if(resultSet!= null)
        resultSet.close()
      connection.close()
    }
  }

  override def setSchema(connection: Connection, schemaName: String): Unit = {
    try {
      connection.setSchema(schemaName)
    }catch {
      case _ => //do nothing
    }
  }

  override def showTables(connection: Connection, schemaName: String): util.List[String] = {
    var rs: ResultSet = null
    try {
      val tables: util.List[String] = new util.ArrayList[String]
      val metadata: DatabaseMetaData = connection.getMetaData
      rs = metadata.getTables(null, schemaName, null, null)
      while (rs.next()) {
        tables.add(rs.getString("TABLE_NAME"))
      }
      tables
    }
    finally {
      if(rs != null)
        rs.close()
      connection.close()
    }
  }

  override def showViews(connection: Connection, schemaName: String): util.List[String] = {
    var rs: ResultSet = null
    try {
      val tables: util.List[String] = new util.ArrayList[String]
      val metadata: DatabaseMetaData = connection.getMetaData
      rs = metadata.getTables(null, schemaName, null, Array("VIEW"))
      while (rs.next()) {
        tables.add(rs.getString("TABLE_NAME"))
      }
      tables
    }
    finally {
      connection.close()
    }
  }

  override def showDatabases(connection: Connection): util.List[String] = {
    try {
      val catalogs = connection.getMetaData.getCatalogs
      val databases: util.List[String] = new util.ArrayList[String]
      while (catalogs.next()) {
        databases.add(catalogs.getString("TABLE_CAT"))
      }
      databases
    }
    finally {
      connection.close()
    }
  }

  override def setDatabase(connection: Connection, database: String): Unit = {
    try {
      connection.setCatalog(database)
    }
    connection.close()
  }

  override def showSchemas(connection: Connection): util.List[String] = {
    var rs: ResultSet = null
    try {
      rs = connection.getMetaData.getSchemas()
      val schemas: util.List[String] = new util.ArrayList[String]()
      while (rs.next()) {
        schemas.add(rs.getString("TABLE_SCHEM"))
      }
      schemas
    }
    finally {
      if(rs != null)
        rs.close()
      connection.close()
    }
  }

  override def getColumnType(typeStr: String): ColumnType = {
    typeStr match  {
      case "array"=>ColumnType.ARRAY
      case "int"=>ColumnType.BIGINT
      case "binary"=>ColumnType.BINARY
      case "bool"=>ColumnType.BOOLEAN
      case "boolean"=>ColumnType.BOOLEAN
      case "bit"=>ColumnType.BOOLEAN
      case "char"=>ColumnType.STRING
      case "date"=>ColumnType.DATE
      case "decimal"=>ColumnType.DECIMAL
      case "double"=>ColumnType.DOUBLE
      case "float"=>ColumnType.FLOAT
      case "integer"=>ColumnType.INT
      case "longvarchar"=>ColumnType.STRING
      case "numeric"=>ColumnType.DECIMAL
      case "nvarchar"=>ColumnType.STRING
      case "smallint"=>ColumnType.INT
      case "timestamp"=>ColumnType.TIMESTAMP
      case "datetime"=>ColumnType.TIMESTAMP
      case "tinyint"=>ColumnType.INT
      case "varchar"=>ColumnType.STRING
      case "varbinary"=>ColumnType.BINARY
      case "string" => ColumnType.STRING
      case whatever =>
        log("try to find type: " + whatever)
        null
      //TODO: complete for other types

    }
  }
}
