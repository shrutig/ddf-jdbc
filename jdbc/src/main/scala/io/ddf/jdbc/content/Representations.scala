package io.ddf.jdbc.content

import io.ddf.content.{Representation, SqlResult, Schema}

case class ViewRepresentation(viewName:String,schema:Schema)
case class EmptyRepresentation(tableName:String,schema:Schema)

object Representations{
  val VIEW_REP = Array(classOf[ViewRepresentation])
}

