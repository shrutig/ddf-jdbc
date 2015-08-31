package io.ddf.jdbc.etl

import java.util

import io.ddf.DDF
import io.ddf.etl.IHandleJoins
import io.ddf.etl.Types.JoinType
import io.ddf.exception.DDFException
import io.ddf.misc.ADDFFunctionalGroupHandler

import scala.collection.JavaConversions._

class JoinHandler(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with IHandleJoins {

  @throws(classOf[DDFException])
  override def join(anotherDDF: DDF, joinTypeParam: JoinType, byColumns: util.List[String], byLeftColumns: util.List[String], byRightColumns: util.List[String]): DDF = {
    val joinType = if (joinTypeParam == null) JoinType.INNER else joinTypeParam
    val leftTableName: String = getDDF.getUri
    val rightTableName: String = anotherDDF.getUri
    val rightColumnNameSet: util.HashSet[String] = new util.HashSet[String]()
    rightColumnNameSet.addAll(anotherDDF.getSchema.getColumns.map(_.getName))

    var columnString: String = ""
    if (byColumns != null && byColumns.nonEmpty) {
      var i: Int = 0
      while (i < byColumns.size) {
        columnString += String.format("lt.%s = rt.%s AND ", byColumns.get(i), byColumns.get(i))
        rightColumnNameSet.remove(byColumns.get(i))
        i = i + 1
      }
    }
    else {
      if (byLeftColumns != null && byRightColumns != null && byLeftColumns.size == byRightColumns.size && byLeftColumns.nonEmpty) {
        var i: Int = 0
        while (i < byLeftColumns.size) {
          columnString += String.format("lt.%s = rt.%s AND ", byLeftColumns.get(i), byRightColumns.get(i))
          rightColumnNameSet.remove(byRightColumns.get(i))
          i = i + 1
        }
      }
      else {
        throw new DDFException(String.format("Left and right column specifications are missing or not compatible"), null)
      }
    }
    columnString = columnString.substring(0, columnString.length - 5)

    val rightSelectColumns: String = rightColumnNameSet.map(name => String.format("rt.%s AS r_%s", name, name)).mkString(",")

    val executeCommand =
      if (JoinType.LEFTSEMI equals joinType) {
        String.format("SELECT lt.* FROM %s lt %s JOIN %s rt ON (%s)", leftTableName, joinType.getStringRepr, rightTableName, columnString)
      }
      else {
        String.format("SELECT lt.*,%s FROM %s lt %s JOIN %s rt ON (%s)", rightSelectColumns, leftTableName, joinType.getStringRepr, rightTableName, columnString)
      }
    this.getManager.sql2ddf(executeCommand)
  }

  override def merge(anotherDDF: DDF): DDF = {
    val sql = String.format("SELECT * from %s UNION ALL SELECT * from %s", ddf.getUri, anotherDDF.getTableName)
    ddf.sql2ddf(sql)
  }
}
