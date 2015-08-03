package io.ddf.jdbc.content

import io.ddf.DDF
import io.ddf.content.AMetaDataHandler

class MetadataHandler(ddf:DDF) extends AMetaDataHandler(ddf){
  override def getNumRowsImpl: Long = {
    val sqlResult = ddf.getSqlHandler.sql(String.format("SELECT count(*) from %s",ddf.getName.toUpperCase))
    sqlResult.getRows.get(0).toLong
  }
}
