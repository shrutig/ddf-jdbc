package io.ddf.jdbc.content

import java.io.FileReader
import java.text.SimpleDateFormat
import java.util.Date

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import io.ddf.DDFManager
import io.ddf.content.Schema.{Column, ColumnType}
import io.ddf.content.{Schema, SqlResult}
import org.apache.commons.io.IOUtils
import scalikejdbc._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.parsing.combinator.{JavaTokenParsers, RegexParsers}
import scala.util.{Failure, Success, Try}

object SqlCommand {

  def apply(db: String, name: String, command: String, maxRows: Int) = {
    val schema = new Schema(name, "")
    val list = NamedDB(db) readOnly { implicit session =>
      SQL(command).map { rs =>
        val actualRS = rs.underlying
        val md = actualRS.getMetaData
        val colCount = md.getColumnCount
        var colIdx = 0
        val row: Array[String] = Array()
        val columns: Array[Column] = Array()
        for (colIdx <- 0 to colCount) {
          row(colIdx) = actualRS.getObject(colIdx).toString
          val colName = md.getColumnName(colIdx)
          val colType = md.getColumnTypeName(colIdx)
          columns(colIdx) = new Column(colName, colType)
        }
        schema.setColumns(columns.toList.asJava)
        row.mkString(",")
      }.list.apply()
    }
    val subList = if (maxRows < list.size) list.take(maxRows) else list
    new SqlResult(schema, subList)
  }
}

object DdlCommand {
  def apply(db: String, command: String) = {
    NamedDB(db) localTx { implicit session =>
      SQL(command).execute().apply()
    }
  }
}

object LoadCommand {
  private val dateFormat = new SimpleDateFormat()

  class SerCsvParserSettings extends CsvParserSettings with Serializable

  def apply(ddfManager: DDFManager, db: String, command: String) = {
    val l = Parsers.parseLoad(command)
    val parser: CsvParser = getParser(l)
    val reader = new FileReader(l.url)
    val lines = parser.parseAll(reader)
    IOUtils.closeQuietly(reader)
    val ddf = ddfManager.getDDFByName(l.tableName)
    val schema = ddf.getSchema
    insert(db, schema, lines, l.useDefaults)
    l.tableName
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

  def insert(db: String, schema: Schema, lines: Seq[Array[String]], useDefaults: Boolean): Seq[Int] = {
    val columns = schema.getColumns
    val colStr = columns.map(col => col.getName).mkString(",")
    val paramStr = columns.map(col => "?").mkString(",")
    NamedDB(db) localTx { implicit session =>
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
    val idxColumns: Seq[(Column, Int)] = columns.zipWithIndex.toSeq
    val row = new Array[Any](idxColumns.size)
    idxColumns foreach {
      case (col, idx) =>
        val colValue: String = getFieldValue(rowArray(idx), col.isNumeric)
        col.getType match {
          case ColumnType.STRING =>
            row(idx) = colValue
          case ColumnType.INT =>
            row(idx) = Try(colValue.toInt).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.FLOAT =>
            row(idx) = Try(colValue.toFloat).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.DOUBLE =>
            row(idx) = Try(colValue.toDouble).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.BIGINT =>
            row(idx) = Try(colValue.toDouble).getOrElse(if (useDefaults) 0 else null)
          case ColumnType.TIMESTAMP =>
            row(idx) = Try(dateFormat.parse(colValue)).getOrElse(new Date(0))
          case ColumnType.BOOLEAN =>
            row(idx) = Try(colValue.toBoolean).getOrElse(if (useDefaults) false else null)
        }
    }
    row
  }


  case class Load(tableName: String, delimiter: Char, url: String, nullValue: String, emptyValue: String, useDefaults: Boolean) extends Function

  case class Create(tableName: String) extends Function

  sealed trait Function

  object Parsers extends RegexParsers with JavaTokenParsers {

    def parseLoad(input: String): Load = parseAll(load, input) match {
      case s: Success[Load] => s.get
      case e: Error =>
        val msg = "Cannot parse [" + input + "] because " + e.msg
        throw new IllegalArgumentException(msg)
      case f: Failure =>
        val msg = "Cannot parse [" + input + "] because " + f.msg
        throw new IllegalArgumentException(msg)
    }

    def parseCreate(input: String): Create = parseAll(create, input) match {
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
          nullValue = nullVal.getOrElse(null),
          emptyValue = emptyVal.getOrElse(null),
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

}
