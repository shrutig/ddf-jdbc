package io.ddf.teradata.content

import java.sql.{DatabaseMetaData, ResultSet, PreparedStatement, Connection}
import java.util
import scala.collection.JavaConversions._
import io.ddf.content.Schema
import io.ddf.jdbc.content.{Catalog, LoadCommand}
import io.ddf.misc.Config
import scalikejdbc.{SQL, ParameterBinder, DB}

object TeradataLoadCommand extends LoadCommand {

  override def insert(connection: Connection,
                      schemaName: String,
                      schema: Schema,
                      lines: Seq[Array[String]],
                      useDefaults: Boolean)
                     (implicit catalog: Catalog): Seq[Int] = {
    val columns = schema.getColumns
    val colStr = columns.map(col => col.getName).mkString(",")
    val paramStr = columns.map(col => "?").mkString(",")
    try {
      val db = DB(connection)
      implicit val session = db autoCommitSession()
      catalog.setSchema(connection, schemaName)
      val columnTypes = listSqlColumnTypesForTable(connection,
        null, schema.getTableName)
      /* The following lines are used in case a null value has to be
      inserted into teradata. In teradata to insert a null value in a
      preparedStatement, we need to specify the type of the column using
      the setObject method. Thus, a ParameterBinder is used to accomplish this.
       */
      db localTx { implicit session =>
        val batchParams: Seq[Seq[Any]] = lines.map(line =>
          parseRow(line, columns, useDefaults))
        val params: Seq[Seq[Any]] = batchParams.map { line =>
          line.map {
            param => if (param == null) {
              ParameterBinder[Any](
                value = param,
                binder = (stmt: PreparedStatement, idx: Int) =>
                  stmt.setObject(idx, param, columnTypes(idx - 1))
              )
            }
            else
              param
          }
        }
        /* database name added in the following statement as teradata throws
        table/view not found error if database name is not specified */
        val sql = s"insert into " +
          Config.getValue("teradata", "jdbcUrl").split("/").last +
          "." + schema.getTableName + " ( " + colStr + " ) values ( " +
          paramStr + ") "
        SQL(sql).batch(params: _*).apply()

      }

    }
    finally {
      connection.close()
    }
  }

  /* This function is used to find the java.Sql.types of the table
  columns in teradata */
  private def listSqlColumnTypesForTable(connection: Connection,
                                 schemaName: String,
                                 tableName: String): util.List[Int] = {
    var resultSet: ResultSet = null
    val columns: util.List[Int] = new util.ArrayList[Int]
    try {
      val metadata: DatabaseMetaData = connection.getMetaData
      resultSet = metadata.getColumns(
        null,
        schemaName,
        tableName.toUpperCase,
        null)
      while (resultSet.next) {
        val columnName = resultSet.getString(4)
        var columnType = resultSet.getInt(5)
        columns.add(columnType)
      }
      columns
    }
    finally {
      if(resultSet != null)
        resultSet.close()
    }
  }
}