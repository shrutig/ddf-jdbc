package io.ddf.jdbc.content

import java.sql.{Connection, DatabaseMetaData, ResultSet, SQLException}
import java.util

import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import scalikejdbc._

trait Catalog {
  def getViewSchema(db: String, name: String): Schema

  def getTableSchema(db: String, name: String): Schema
}

object SimpleCatalog extends Catalog {
  def getViewSchema(db: String, name: String): Schema = {
    getTableSchema(db, name)
  }

  def getTableSchema(db: String, name: String): Schema = {
    using(ConnectionPool(db).borrow()) { conn: Connection =>
      val columns = listColumnsForTable(conn, name)
      new Schema(name, columns)
    }
  }

  @throws(classOf[SQLException])
  def listColumnsForTable(connection: Connection, tableName: String): util.List[Column] = {
    val columns: util.List[Column] = new util.ArrayList[Column]
    val metadata: DatabaseMetaData = connection.getMetaData
    val resultSet: ResultSet = metadata.getColumns(null, null, tableName.toUpperCase, null)
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
}
