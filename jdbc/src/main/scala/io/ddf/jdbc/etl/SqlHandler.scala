package io.ddf.jdbc.etl

import java.io.StringReader
import java.util.Collections

import io.ddf.content.{Schema, SqlResult, SqlTypedResult}
import io.ddf.datasource.{DataFormat, DataSourceDescriptor, JDBCDataSourceDescriptor, SQLDataSourceDescriptor}
import io.ddf.exception.DDFException
import io.ddf.jdbc.JdbcDDFManager
import io.ddf.jdbc.content._
import io.ddf.{DDF, TableNameReplacer}
import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.Select
import org.apache.commons.lang.StringUtils


class SqlHandler(ddf: DDF) extends io.ddf.etl.ASqlHandler(ddf) {

  val ddfManager: JdbcDDFManager = ddf.getManager.asInstanceOf[JdbcDDFManager]
  val baseSchema = ddfManager.baseSchema

  implicit val catalog = ddfManager.catalog
  val connection = ddfManager.connection

  override def sql2ddf(command: String, schema: Schema, dataSource: DataSourceDescriptor, dataFormat: DataFormat): DDF = {
    ddfManager.checkSinkAllowed()
    if (StringUtils.startsWithIgnoreCase(command.trim, "LOAD")) {
      load(command)
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "CREATE")) {
      create2ddf(command, schema)
    } else {
      if (this.ddfManager.getCanCreateView()) {
        val viewName = genTableName(8)
        //View will allow select commands
        DdlCommand(connection, baseSchema, "CREATE VIEW " + viewName + " AS (" + command + ")")
        val viewSchema = if (schema == null) catalog.getViewSchema(connection, baseSchema, viewName) else schema
        val viewRep = TableNameRepresentation(viewName, viewSchema)
        ddf.getManager.newDDF(this.getManager, viewRep, Array(Representations.VIEW), this.getManager.getEngineName, ddf.getNamespace, viewName, viewSchema)
      } else {

      }
    }
  }

  def load(command: String): DDF = {
    val l = LoadCommand.parse(command)
    val ddf = ddfManager.getDDFByName(l.tableName)
    val schema = ddf.getSchema
    val tableName = LoadCommand(connection, baseSchema, schema, l)
    val newDDF = ddfManager.getDDFByName(tableName)
    newDDF
  }

  def create2ddf(command: String, schema: Schema): DDF = {
    val tableName = Parsers.parseCreate(command).tableName
    DdlCommand(connection, baseSchema, command)
    val tableSchema = if (schema == null) catalog.getTableSchema(connection, baseSchema, tableName) else schema
    val emptyRep = TableNameRepresentation(tableName, tableSchema)
    ddf.getManager.newDDF(this.getManager, emptyRep, Array(Representations.VIEW), this.getManager.getEngineName, ddf.getNamespace, tableName, tableSchema)
  }

  override def sql(command: String): SqlResult = {
    sql(command, Integer.MAX_VALUE, null)
  }

  override def sql(command: String, maxRows: Integer): SqlResult = {
    sql(command, maxRows, null)
  }

  override def sql(command: String, maxRows: Integer, dataSource: DataSourceDescriptor): SqlResult = {
    this.ddfManager.log("run sql in ddf-jdbc, command is : " + command)
    val maxRowsInt: Int = if (maxRows == null) Integer.MAX_VALUE else maxRows
    if (StringUtils.startsWithIgnoreCase(command.trim, "DROP")) {
      DdlCommand(connection, baseSchema, command)
      new SqlResult(null, Collections.singletonList("0"))
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "LOAD")) {
      ddfManager.checkSinkAllowed()
      val l = LoadCommand.parse(command)
      val ddf = ddfManager.getDDFByName(l.tableName)
      val schema = ddf.getSchema
      val tableName = LoadCommand(connection, baseSchema, schema, l)
      new SqlResult(null, Collections.singletonList(tableName))
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "CREATE")) {
      create2ddf(command, null)
      new SqlResult(null, Collections.singletonList("0"))
    } else {
      val tableName = ddf.getSchemaHandler.newTableName()
      SqlCommand(connection, baseSchema, tableName, command, maxRowsInt,
        "\t", this.ddfManager.getEngine)
    }
  }

  val possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  val possibleText = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

  def genTableName(length: Int) = {
    def random(possible: String) = possible.charAt(Math.floor(Math.random() * possible.length).toInt)
    val text = new StringBuffer
    var i = 0
    while (i < length) {
      if (i == 0)
        text.append(random(possibleText))
      else
        text.append(random(possible))
      i = i + 1
    }
    text.toString
  }

  override def sqlTyped(command: String): SqlTypedResult = new SqlTypedResult(sql(command))

  override def sqlTyped(command: String, maxRows: Integer): SqlTypedResult = new SqlTypedResult(sql(command, maxRows))

  override def sqlTyped(command: String, maxRows: Integer, dataSource: DataSourceDescriptor): SqlTypedResult = new SqlTypedResult(sql(command, maxRows, dataSource))
}
