package io.ddf.jdbc.etl

import java.util.Collections

import io.ddf.DDF
import io.ddf.content.{Schema, SqlResult}
import io.ddf.datasource.DataFormat
import io.ddf.jdbc.JdbcDDFManager
import io.ddf.jdbc.content._
import org.apache.commons.lang.StringUtils


class SqlHandler(ddf: DDF) extends io.ddf.etl.ASqlHandler(ddf) {

  val ddfManager: JdbcDDFManager = ddf.getManager.asInstanceOf[JdbcDDFManager]
  val baseSchema = ddfManager.baseSchema

  //Override where required
  protected def defaultDataSourceName = ddfManager.defaultDataSourceName

  //Override where required
  protected def catalog: Catalog = SimpleCatalog

  override def sql2ddf(command: String): DDF = {
    this.sql2ddf(command, null, defaultDataSourceName, null)
  }

  override def sql2ddf(command: String, schema: Schema): DDF = {
    this.sql2ddf(command, schema, defaultDataSourceName, null)
  }

  override def sql2ddf(command: String, dataFormat: DataFormat): DDF = {
    this.sql2ddf(command, null, defaultDataSourceName, null)
  }

  override def sql2ddf(command: String, schema: Schema, dataSource: String): DDF = {
    this.sql2ddf(command, schema, dataSource, null)
  }

  override def sql2ddf(command: String, schema: Schema, dataFormat: DataFormat): DDF = {
    this.sql2ddf(command, schema, defaultDataSourceName, null)
  }

  override def sql2ddf(command: String, schema: Schema, dataSource: String, dataFormat: DataFormat): DDF = {
    val dbName = if (dataSource == null) defaultDataSourceName else dataSource
    if (StringUtils.startsWithIgnoreCase(command.trim, "LOAD")) {
      val tableName = LoadCommand(ddfManager, dbName, command)
      val newDDF = ddfManager.getDDFByName(tableName)
      newDDF
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "CREATE")) {
      val tableName = Parsers.parseCreate(command).tableName
      DdlCommand(dbName, command)
      val tableSchema = if (schema == null) catalog.getTableSchema("remote", baseSchema, tableName) else schema
      val emptyRep = TableNameRepresentation(tableName, tableSchema)
      ddf.getManager.newDDF(this.getManager, emptyRep, Array(Representations.VIEW), ddf.getNamespace, tableName, tableSchema)
    } else {
      val viewName = ddf.getSchemaHandler.newTableName()
      //View will allow select commands
      DdlCommand(dbName, "CREATE VIEW " + viewName + " AS (" + command + ")")
      val viewSchema = if (schema == null) catalog.getViewSchema("remote", baseSchema, viewName) else schema
      val viewRep = TableNameRepresentation(viewName, viewSchema)
      ddf.getManager.newDDF(this.getManager, viewRep, Array(Representations.VIEW), ddf.getNamespace, viewName, viewSchema)
    }
  }

  override def sql(command: String): SqlResult = {
    sql(command, Integer.MAX_VALUE, defaultDataSourceName)
  }

  override def sql(command: String, maxRows: Integer): SqlResult = {
    sql(command, Integer.MAX_VALUE, defaultDataSourceName)
  }

  override def sql(command: String, maxRows: Integer, dataSource: String): SqlResult = {
    val maxRowsInt: Int = if (maxRows == null) Integer.MAX_VALUE else maxRows
    val dbName = if (dataSource == null) defaultDataSourceName else dataSource
    if (StringUtils.startsWithIgnoreCase(command.trim, "DROP")) {
      DdlCommand(dbName, command)
      new SqlResult(null, Collections.singletonList("0"))
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "LOAD")) {
      LoadCommand(ddfManager, dbName, command)
      new SqlResult(null, Collections.singletonList("0"))
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "CREATE")) {
      sql2ddf(command, null, dbName)
      new SqlResult(null, Collections.singletonList("0"))
    } else {
      val tableName = ddf.getSchemaHandler.newTableName()
      SqlCommand(dbName, tableName, command, maxRowsInt, "\t")
    }
  }


}
