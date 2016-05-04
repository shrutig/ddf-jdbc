package io.ddf.teradata.etl

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.datasource.{DataFormat, DataSourceDescriptor}
import io.ddf.jdbc.content._
import io.ddf.teradata.TeradataDDFManager
import org.apache.commons.lang.StringUtils


class SqlHandler(ddf: DDF) extends io.ddf.jdbc.etl.SqlHandler(ddf) {

  override def sql2ddf(command: String,
                       schema: Schema,
                       dataSource: DataSourceDescriptor,
                       dataFormat: DataFormat): DDF = {
    ddfManager.checkSinkAllowed()
    if (StringUtils.startsWithIgnoreCase(command.trim, "LOAD")) {
      load(command)
    } else if (StringUtils.startsWithIgnoreCase(command.trim, "CREATE")) {
      create2ddf(command, schema)
    } else {
      if (this.ddfManager.getCanCreateView()) {
        val viewName = TableNameGenerator.genTableName(8)
        //View will allow select commands
        /*
      Here, viewName is appended with a database
       name taken from config file
       */
        val stmt = "CREATE VIEW " +
          this.getManager.asInstanceOf[TeradataDDFManager].db + "." +
          viewName + " AS (" + command + ")"
        DdlCommand(
          getConnection(),
          baseSchema,
          stmt)
        val viewSchema = if (schema == null)
          catalog.getViewSchema(getConnection(), baseSchema, viewName)
        else
          schema
        val viewRep = TableNameRepresentation(viewName, viewSchema)
        // TODO(TJ): This function implementation is wrong.
        ddf.getManager.newDDF(
          this.getManager,
          viewRep,
          Array(Representations.VIEW),
          viewName,
          viewSchema)
      } else {
        val sqlRet = this.sql("select * from (" + command + ") tmp limit 1")
        val schema = sqlRet.getSchema
        val viewName = TableNameGenerator.genTableName(8)
        schema.setTableName(command)
        val newDDF = ddf.getManager.newDDF(
          this.getManager, // the ddfmanager
          "this is a view", // the content
          // content class
          Array(classOf[java.lang.String]),
          null,
          schema)
        // Indicate that this ddf is a view, this information will be handled
        // in TableNameReplacer
        newDDF.setIsDDFView(true)
        newDDF
      }
    }
  }

}