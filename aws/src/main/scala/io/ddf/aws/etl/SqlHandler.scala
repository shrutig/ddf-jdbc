package io.ddf.aws.etl

import java.util

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.jdbc.content.{Catalog, SqlArrayResultCommand}

class SqlHandler(ddf: DDF) extends io.ddf.jdbc.etl.SqlHandler(ddf) {

  override def catalog = RedshiftCatalog
}

object RedshiftCatalog extends Catalog {
  override def getViewSchema(db: String, schemaName: String, name: String): Schema = getTableSchema(db, schemaName, name)

  override def getTableSchema(db: String, schemaName: String, name: String): Schema = {
    val sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '" + schemaName.toLowerCase + "' AND table_name ='" + name.toLowerCase + "' order by ordinal_position"
    val columns: util.List[Column] = new util.ArrayList[Column]
    val sqlResult = SqlArrayResultCommand(db, name, sql)
    sqlResult.result.foreach { row =>
      val columnName = row(0).toString
      var columnTypeStr = row(1).toString
      if (columnTypeStr.equalsIgnoreCase("character varying") || "text".equalsIgnoreCase(columnTypeStr) || "VARCHAR".equalsIgnoreCase(columnTypeStr) || "VARCHAR2".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "STRING"
      }
      if ("double precision".equalsIgnoreCase(columnTypeStr)) {
        columnTypeStr = "DOUBLE"
      }
      val column = new Column(columnName, columnTypeStr)
      columns.add(column)
    }
    new Schema(name, columns)
  }

}
