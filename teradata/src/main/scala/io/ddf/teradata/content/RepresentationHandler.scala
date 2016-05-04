package io.ddf.teradata.content

import io.ddf.DDF
import io.ddf.content.{RepresentationHandler => RH, _}
import io.ddf.jdbc.content.{SqlArrayResult, TableNameRepresentation, SqlArrayResultCommand}
import io.ddf.teradata.TeradataDDFManager

class RepresentationHandler(ddf: DDF) extends RH(ddf) {

  import Representations._

  override def getDefaultDataType: Array[Class[_]] = Array(Representations.VIEW)

  this.addConvertFunction(VIEW_REP, SQL_ARRAY_RESULT_REP,
    new View2SqlArrayResult(this.ddf))
}


object Representations {

  val VIEW = classOf[TableNameRepresentation]
  val SQL_ARRAY_RESULT = classOf[SqlArrayResult]

  val SQL_ARRAY_RESULT_REP = new Representation(SQL_ARRAY_RESULT)
  val VIEW_REP = new Representation(VIEW)

  class View2SqlArrayResult(@transient ddf: DDF) extends ConvertFunction(ddf) {
    val ddfManager = ddf.getManager.asInstanceOf[TeradataDDFManager]
    implicit val catalog = ddfManager.catalog

    override def apply(representation: Representation): Representation = {
      val view = representation.getValue.asInstanceOf[TableNameRepresentation]
      //Override where required
      val sqlArrayResult = SqlArrayResultCommand(ddfManager.getConnection(),
        ddfManager.baseSchema,
        ddf.getName,
        /*here, ddf.getTableName is used(which returns tableName in the form
         of db.table to prevent view or table not found exception in teradata  */
        String.format("SELECT * FROM %s", ddf.getTableName()))
      new Representation(new SqlArrayResult(view.schema,
        sqlArrayResult.result), SQL_ARRAY_RESULT)
    }
  }

}

