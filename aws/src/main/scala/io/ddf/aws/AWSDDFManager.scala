package io.ddf.aws

import java.util

import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.jdbc.JdbcDDFManager
import io.ddf.jdbc.content.{SqlArrayResultCommand, Catalog}
import scalikejdbc.{SQL, DBSession}

class AWSDDFManager extends JdbcDDFManager {
  override def getEngine = "aws"
  override def catalog = RedshiftCatalog
}


object RedshiftCatalog extends Catalog {
  override def getViewSchema(db: String, schemaName: String, name: String): Schema = getTableSchema(db, schemaName, name)

  override def getTableSchema(db: String, schemaName: String, name: String): Schema = {
    implicit val catalog = this
    val sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '" + schemaName.toLowerCase + "' AND table_name ='" + name.toLowerCase + "' order by ordinal_position"
    val columns: util.List[Column] = new util.ArrayList[Column]
    val sqlResult = SqlArrayResultCommand(db,"information_schema", name, sql)
    sqlResult.result.foreach { row =>
      val columnName = row(0).toString
      var columnTypeStr = row(1).toString
      if (columnTypeStr.equalsIgnoreCase("character varying") || "text".equalsIgnoreCase(columnTypeStr) || "VARCHAR".equalsIgnoreCase(columnTypeStr) || "VARCHAR2".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "STRING"
      }
      if ("double precision".equalsIgnoreCase(columnTypeStr) || "decimal".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "DOUBLE"
      }
      if ("numeric".equalsIgnoreCase(columnTypeStr) || "serial".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "INTEGER"
      }

      val column = new Column(columnName, columnTypeStr)
      columns.add(column)
    }
    new Schema(name, columns)
  }

  override def setSchema(schemaName: String)(implicit session: DBSession): Unit ={
    SQL("set search_path to "+schemaName).executeUpdate().apply()
  }
}
