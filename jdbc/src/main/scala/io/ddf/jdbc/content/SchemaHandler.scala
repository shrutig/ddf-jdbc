package io.ddf.jdbc.content

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.exception.DDFException

class SchemaHandler(ddf: DDF) extends io.ddf.content.SchemaHandler(ddf: DDF) {

  @throws(classOf[DDFException])
  override def setFactorLevelsForStringColumns(xCols: Array[String]) {
    xCols.foreach { col =>
      val c = this.getColumn(col)
      if (c.getType eq Schema.ColumnType.STRING) {
        this.setAsFactor(c.getName)
      }
    }
  }
}
