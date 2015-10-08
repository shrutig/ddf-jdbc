package io.ddf.aws

import io.ddf.datasource.DataSourceDescriptor
import io.ddf.jdbc.content.Parsers
import io.ddf.postgres.PostgresDDFManager
import io.ddf.DDFManager.EngineType
class AWSDDFManager(dataSourceDescriptor: DataSourceDescriptor, engineType: EngineType) extends PostgresDDFManager(dataSourceDescriptor, engineType) {
  override def getEngine:String = engineType.name()
}
