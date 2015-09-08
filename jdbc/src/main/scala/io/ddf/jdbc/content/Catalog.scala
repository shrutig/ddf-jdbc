package io.ddf.jdbc.content

import java.sql.{Connection, DatabaseMetaData, ResultSet, SQLException}
import java.util

import io.ddf.content.Schema
import io.ddf.content.Schema.Column

trait Catalog {
  def getViewSchema(connection: Connection, schemaName: String, tableName: String): Schema

  def getTableSchema(connection: Connection, schemaName: String, tableName: String): Schema

  def listColumnsForTable(connection: Connection,
                          schemaName: String,
                          tableName: String): util.List[Column]

  def setSchema(connection: Connection, schemaName: String)

  def showTables(connection: Connection, schemaName: String): util.List[String]

  def showDatabases(connection: Connection) : util.List[String]

  def setDatabase(connection: Connection, database : String)
}

object SimpleCatalog extends Catalog {
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
    val columns: util.List[Column] = new util.ArrayList[Column]
    val metadata: DatabaseMetaData = connection.getMetaData
    val resultSet: ResultSet = metadata.getColumns(null, schemaName, tableName.toUpperCase, null)
    while (resultSet.next) {
      val columnName = resultSet.getString(4)
      var columnTypeStr = resultSet.getString(6)
      if ("VARCHAR".equalsIgnoreCase(columnTypeStr) || "VARCHAR2".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "STRING"
      }
      val column = new Column(columnName, columnTypeStr)
      columns.add(column)
    }
    columns
  }

  override def setSchema(connection: Connection, schemaName: String): Unit = {
    //do nothing
  }

  override def showTables(connection: Connection, schemaName: String): util.List[String] = {
    val tables: util.List[String] = new util.ArrayList[String]
    val metadata: DatabaseMetaData = connection.getMetaData
    val rs: ResultSet = metadata.getTables(null, schemaName, null, null)
    while (rs.next()) {
      tables.add(rs.getString("TABLE_NAME"))
    }
    tables
  }

  override def showDatabases(connection: Connection): util.List[String] = {
    val catalogs = connection.getMetaData.getCatalogs
    val databases: util.List[String] = new util.ArrayList[String]
    while (catalogs.next()) {
      databases.add(catalogs.getString("TABLE_CAT"))
    }
    databases
  }

  override def setDatabase(connection: Connection, database: String): Unit = {
    connection.setCatalog(database)
  }
}
