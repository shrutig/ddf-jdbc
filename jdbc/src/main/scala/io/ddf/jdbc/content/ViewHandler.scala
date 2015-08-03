package io.ddf.jdbc.content

import java.util

import com.google.common.collect.Lists
import io.ddf.DDF

import scala.collection.JavaConversions._


class ViewHandler(ddf: DDF) extends io.ddf.content.ViewHandler(ddf) {

  override def removeColumns(columnNames: util.List[String]): DDF = {
    val columns = ddf.getSchema.getColumns.map { col => col.getName }
    val result: util.List[String] = Lists.newArrayList()
    result.addAll(columns)
    columnNames.foreach { columnName => {
      val it: Iterator[String] = result.iterator
      while (it.hasNext) {
        if (it.next.equalsIgnoreCase(columnName)) {
          it.remove
        }
      }
    }
    }
    val newddf: DDF = this.project(result)
    newddf.getMetaDataHandler.copyFactor(this.getDDF)
    newddf
  }
}
