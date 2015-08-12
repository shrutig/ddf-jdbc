package io.ddf.jdbc.content

import java.io.FileReader
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.google.common.collect.Lists
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import io.ddf.DDFManager
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.content.{Schema, SqlResult}
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import scalikejdbc._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.parsing.combinator.{JavaTokenParsers, RegexParsers}
import scala.util.{Failure, Success, Try}

object SqlCommand {

  private final val logger = LoggerFactory.getLogger(getClass)

  def apply(db: String, schemaName: String, tableName: String, command: String, maxRows: Int, separator: String)(implicit catalog: Catalog) = {
    val schema = new Schema(tableName, "")
    val list = NamedDB(db) localTx { implicit session =>
      catalog.setSchema(schemaName)
      SQL(command).map { rs =>
        val actualRS = rs.underlying
        val md = actualRS.getMetaData
        val colCount = md.getColumnCount
        val row: Array[String] = new Array[String](colCount)
        val columns: Array[Column] = new Array[Column](colCount)
        var colIdx = 0
        while (colIdx < colCount) {
          //resultset in jdbc start at 1
          val rsIdx = colIdx + 1
          val obj = actualRS.getObject(rsIdx)
          row(colIdx) = if (obj == null) null else obj.toString
          val colName = md.getColumnName(rsIdx)
          val colType = md.getColumnTypeName(rsIdx)
          columns(colIdx) = new Column(colName, colType)
          colIdx = colIdx + 1
        }
        schema.setColumns(columns.toList.asJava)
        val rowStr = row.mkString(separator)
        rowStr
      }.list().apply
    }
    val subList = if (maxRows < list.size) list.take(maxRows) else list
    new SqlResult(schema, subList)
  }
}

object SqlArrayResultCommand {

  def apply(db: String, schemaName: String, tableName: String, command: String)(implicit catalog: Catalog): SqlArrayResult = {
    apply(db, schemaName, tableName, command, Integer.MAX_VALUE)
  }

  def apply(db: String, schemaName: String, tableName: String, command: String, maxRows: Int)(implicit catalog: Catalog): SqlArrayResult = {
    val schema = new Schema(tableName, "")
    val list = NamedDB(db) localTx { implicit session =>
      catalog.setSchema(schemaName)
      SQL(command).map { rs =>
        val actualRS = rs.underlying
        val md = actualRS.getMetaData
        val colCount = md.getColumnCount
        val row: Array[Any] = new Array[Any](colCount)
        val columns: Array[Column] = new Array[Column](colCount)
        var colIdx = 0
        while (colIdx < colCount) {
          //resultset in jdbc start at 1
          val rsIdx = colIdx + 1
          row(colIdx) = actualRS.getObject(rsIdx)
          val colName = md.getColumnName(rsIdx)
          val colType = md.getColumnTypeName(rsIdx)
          columns(colIdx) = new Column(colName, colType)
          colIdx = colIdx + 1
        }
        schema.setColumns(columns.toList.asJava)
        row
      }.list().apply
    }
    val subList = if (maxRows < list.size) list.take(maxRows) else list
    new SqlArrayResult(schema, subList)
  }
}


object DdlCommand {
  def apply(db: String, schemaName: String, command: String)(implicit catalog: Catalog) = {
    NamedDB(db) autoCommit { implicit session =>
      catalog.setSchema(schemaName)
      SQL(command).execute().apply()
    }
  }
}

object SchemaToCreate {
  def apply(db: String, schema: Schema) = {
    val command = "CREATE TABLE " + schema.getTableName + " (" + schema.getColumns.map { col =>
      val sqlType = col.getType match {
        case ColumnType.STRING => "VARCHAR"
        case _ => col.getType
      }
      col.getName + " " + sqlType
    }.mkString(",") + ");"
    command
  }
}

object LoadCommand {
  private val dateFormat = new SimpleDateFormat()

  class SerCsvParserSettings extends CsvParserSettings with Serializable

  def apply(ddfManager: DDFManager, db: String, schemaName: String, command: String)(implicit catalog: Catalog): String = {
    val l = Parsers.parseLoad(command)
    apply(ddfManager, db, schemaName, l)
  }

  def apply(ddfManager: DDFManager, db: String, schemaName: String, l: Load)(implicit catalog: Catalog): String = {
    val lines: util.List[Array[String]] = getLines(l)
    val ddf = ddfManager.getDDFByName(l.tableName)
    val schema = ddf.getSchema
    insert(db, schemaName, schema, lines, l.useDefaults)
    l.tableName
  }

  def getLines(l: Load): util.List[Array[String]] = {
    val parser: CsvParser = getParser(l)
    val reader = new FileReader(l.url)
    val lines = parser.parseAll(reader)
    IOUtils.closeQuietly(reader)
    lines
  }

  def getLines(l: Load, maxRows: Int): util.List[Array[String]] = {
    val parser: CsvParser = getParser(l)
    val reader = new FileReader(l.url)
    var parsedRows = 0
    val rows = Lists.newArrayList[Array[String]]()
    parser.beginParsing(reader)
    while (parsedRows < maxRows) {
      val row = parser.parseNext()
      if (row != null) {
        rows.add(row)
        parsedRows = parsedRows + 1
      }
      else parsedRows = maxRows
    }
    rows
  }

  def getParser(l: Load): CsvParser = {
    val parserSettings = new SerCsvParserSettings()
    parserSettings.setIgnoreLeadingWhitespaces(false)
    parserSettings.setIgnoreTrailingWhitespaces(false)
    parserSettings.getFormat.setDelimiter(l.delimiter)
    if (l.emptyValue != null)
      parserSettings.setEmptyValue(l.emptyValue)
    if (l.nullValue != null)
      parserSettings.setNullValue(l.nullValue)
    val parser = new CsvParser(parserSettings)
    parser
  }

  def insert(db: String, schemaName: String, schema: Schema, lines: Seq[Array[String]], useDefaults: Boolean)(implicit catalog: Catalog): Seq[Int] = {
    val columns = schema.getColumns
    val colStr = columns.map(col => col.getName).mkString(",")
    val paramStr = columns.map(col => "?").mkString(",")
    NamedDB(db) localTx { implicit session =>
      catalog.setSchema(schemaName)
      val batchParams: Seq[Seq[Any]] = lines.map(line => parseRow(line, columns, useDefaults))
      val sql = "insert into " + schema.getTableName + " (" + colStr + ") values (" + paramStr + ") "
      SQL(sql).batch(batchParams: _*).apply()
    }
  }

  private def getFieldValue(elem: String, isNumeric: Boolean): String = {
    val mayBeString: Try[String] = Try(elem.toString.trim)
    mayBeString match {
      case Success(s) if isNumeric && s.equalsIgnoreCase("NA") => null
      case Success(s) => s
      case Failure(e) if isNumeric => null
      case Failure(e) => null
    }
  }

  private def parseRow(rowArray: Array[String], columns: Seq[Column], useDefaults: Boolean): Seq[_] = {
    val idxColumns: Seq[(Column, Int)] = columns.zipWithIndex
    val row = new Array[Any](idxColumns.size)
    idxColumns foreach {
      case (col, idx) =>
        val colValue: String = getFieldValue(rowArray(idx), col.isNumeric)
        col.getType match {
          case ColumnType.STRING =>
            row(idx) = colValue
          case ColumnType.INT =>
            row(idx) = Try(colValue.toInt).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.SMALLINT =>
            row(idx) = Try(colValue.toInt).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.TINYINT =>
            row(idx) = Try(colValue.toInt).getOrElse(if (useDefaults) 0 else null)

          case ColumnType.FLOAT =>
            row(idx) = Try(colValue.toFloat).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.DOUBLE =>
            row(idx) = Try(colValue.toDouble).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.DECIMAL =>
            row(idx) = Try(colValue.toDouble).getOrElse(if (useDefaults) 0 else null)

          case ColumnType.BIGINT =>
            row(idx) = Try(colValue.toLong).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.LONG =>
            row(idx) = Try(colValue.toLong).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.TIMESTAMP =>
            row(idx) = Try(dateFormat.parse(colValue)).getOrElse(new Date(0))
          case ColumnType.DATE =>
            row(idx) = Try(dateFormat.parse(colValue)).getOrElse(new Date(0))

          case ColumnType.BOOLEAN =>
            row(idx) = Try(colValue.toBoolean).getOrElse(if (useDefaults) false else null)
        }
    }
    row
  }

}


case class Load(tableName: String, delimiter: Char, url: String, nullValue: String, emptyValue: String, useDefaults: Boolean) extends Function

case class Create(tableName: String) extends Function

sealed trait Function

object Parsers extends RegexParsers with JavaTokenParsers {

  def parseLoad(input: String): Load = parseAll(load, StringUtils.removeEnd(input, ";")) match {
    case s: Success[Load] => s.get
    case e: Error =>
      val msg = "Cannot parse [" + input + "] because " + e.msg
      throw new IllegalArgumentException(msg)
    case f: Failure =>
      val msg = "Cannot parse [" + input + "] because " + f.msg
      throw new IllegalArgumentException(msg)
  }

  def parseCreate(input: String): Create = parseAll(create, StringUtils.removeEnd(input, ";")) match {
    case s: Success[Create] => s.get
    case e: Error =>
      val msg = "Cannot parse [" + input + "] because " + e.msg
      throw new IllegalArgumentException(msg)
    case f: Failure =>
      val msg = "Cannot parse [" + input + "] because " + f.msg
      throw new IllegalArgumentException(msg)
  }


  lazy val load: Parser[Load] =
    (LOAD ~> quotedStr) ~ (DELIMITED ~> BY ~> quotedStr).? ~ (WITH ~> NULL ~> quotedStr).? ~ (WITH ~> EMPTY ~> quotedStr).? ~ (NO ~> DEFAULTS).? ~ (INTO ~> ident) ^^ { case url ~ dl ~ nullVal ~ emptyVal ~ noDef ~ name =>
      Load(name, dl.getOrElse(",").toCharArray()(0), url,
        nullValue = nullVal.orNull,
        emptyValue = emptyVal.orNull,
        noDef.map {
          case s: String => false
          case _ => true
        }.getOrElse(true))
    }

  lazy val create: Parser[Create] = CREATE ~ (TABLE | VIEW) ~ (IF ~> NOT ~> EXISTS).? ~ ident ~ columns ^^ {
    case c ~ tv ~ e ~ tableName ~ cols => Create(tableName)
  }

  def columns: Parser[Map[String, Any]] = "(" ~> repsep(column, ",") <~ ")" ^^ {
    Map() ++ _
  }

  def column: Parser[(String, Any)] =
    ident ~ ident ^^ { case columnName ~ dataType => (columnName, dataType) }


  protected lazy val quotedStr: Parser[String] =
    ("'" + """([^'\p{Cntrl}\\]|\\[\\'"bfnrt]|\\u[a-fA-F0-9]{4})*""" + "'").r ^^ {
      str => str.substring(1, str.length - 1)
    }

  protected val DELIMITED = Keyword("DELIMITED")
  protected val WITH = Keyword("WITH")
  protected val NULL = Keyword("NULL")
  protected val EMPTY = Keyword("EMPTY")
  protected val NO = Keyword("NO")
  protected val DEFAULTS = Keyword("DEFAULTS")
  protected val BY = Keyword("BY")
  protected val LOAD = Keyword("LOAD")
  protected val CREATE = Keyword("CREATE")
  protected val TABLE = Keyword("TABLE")
  protected val IF = Keyword("IF")
  protected val NOT = Keyword("NOT")
  protected val EXISTS = Keyword("EXISTS")
  protected val VIEW = Keyword("VIEW")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val INTO = Keyword("INTO")

  case class Keyword(key: String)

  // Convert the keyword into an case insensitive Parser
  implicit def keyword2Parser(kw: Keyword): Parser[String] = {
    ("""(?i)\Q""" + kw.key + """\E""").r
  }

}
