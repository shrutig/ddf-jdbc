package io.ddf.sfdc.etl

import java.util

import io.ddf.DDF
import io.ddf.content.{Schema, SqlResult}
import io.ddf.datasource.DataSourceDescriptor
import io.ddf.jdbc.JdbcDDFManager

class SqlHandler(ddf: DDF) extends io.ddf.jdbc.etl.SqlHandler(ddf) {

  override def sql(command: String, maxRows: Integer, dataSource: DataSourceDescriptor): SqlResult = {
    if (command.toLowerCase().trim().equals("show tables")) {
      return this.showSFDCTables();
    } else {
      return super.sql(command, maxRows, dataSource);
    }
  }

 def  showSFDCTables(): SqlResult = {
    val tblList:java.util.List[String] =
      (this.getManager().asInstanceOf[JdbcDDFManager]).showTables(null)
    val columnList: util.List[Schema.Column] = new util.ArrayList[Schema.Column]
    columnList.add(new Schema.Column("table_name", Schema.ColumnType.STRING));
    val schema : Schema = new Schema("tables", columnList);
    return new SqlResult(schema, tblList);

    // TODO: what should we return here.
    // return new SqlResult(null, new ArrayList<String>());
  }

}
