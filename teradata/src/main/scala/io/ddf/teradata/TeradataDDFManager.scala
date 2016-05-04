package io.ddf.teradata

import io.ddf.DDF
import io.ddf.DDFManager.EngineType
import io.ddf.content.Schema
import io.ddf.content.Schema.ColumnType
import io.ddf.datasource.DataSourceDescriptor
import io.ddf.jdbc.JdbcDDFManager
import io.ddf.jdbc.content._
import scala.collection.JavaConversions._
import io.ddf.jdbc.etl.SqlHandler
import io.ddf.misc.Config
import io.ddf.teradata.content.TeradataLoadCommand

class TeradataDDFManager(dataSourceDescriptor: DataSourceDescriptor,
                         engineType: EngineType)
  extends JdbcDDFManager(dataSourceDescriptor, engineType){

  override def load(command: String) = {
    checkSinkAllowed()
    val l = TeradataLoadCommand.parse(command)
    val ddf = getDDFByName( l.tableName)
    val schema = ddf.getSchema
    implicit val cat = catalog
    TeradataLoadCommand(getConnection(), baseSchema, schema, l)
    ddf
  }

  lazy val db  = Config.getValue("teradata", "jdbcUrl").split("/").last

  override def create(command: String) = {
    checkSinkAllowed()
    val sqlHandler = this.getDummyDDF.getSqlHandler.asInstanceOf[SqlHandler]
    checkSinkAllowed()
    implicit val cat = catalog
    //Use database executed in the following statement to cause table creation
    // in correct database
    DdlCommand(getConnection(), baseSchema, "database " + db)
    sqlHandler.create2ddf(command, null)
  }

  override def loadFile(fileURL: String, fieldSeparator: String): DDF = {
    checkSinkAllowed()
    implicit val cat = catalog
    val tableName = getDummyDDF.getSchemaHandler.newTableName()
    val load = new Load(
      tableName,
      fieldSeparator.charAt(0),
      fileURL,
      null,
      null,
      true)
    val lines = TeradataLoadCommand.getLines(load, 5)
    import scala.collection.JavaConverters._
    val colInfo = getColumnInfo(lines.asScala.toList,
      hasHeader = false, doPreferDouble = true)
    val schema = new Schema(tableName, colInfo)
    val createCommand = SchemaToCreate(schema)
    val ddf = create(createCommand)
    TeradataLoadCommand(getConnection(), baseSchema, schema, load)
    ddf
  }

  object SchemaToCreate {
    def apply(schema: Schema) = {
      val command = "CREATE TABLE " + schema.getTableName +
        " (" + schema.getColumns.map { col =>
        val sqlType = col.getType match {
            /* VARCHAR type without length is not supported in teradata.
            Hence, VARCHAR in the following line used in jdbc module is
            replaced with VARCHAR(255)
             */
          case ColumnType.STRING => "VARCHAR(255)"
          case ColumnType.DOUBLE => "double precision"
          case _ => col.getType
        }
        col.getName + " " + sqlType
      }.mkString(",") + ");"
      command
    }
  }

}

