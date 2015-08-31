package io.ddf.aws

import io.ddf.datasource.DataSourceDescriptor
import io.ddf.postgres.PostgresDDFManager

class AWSDDFManager(dataSourceDescriptor: DataSourceDescriptor, engineName: String) extends PostgresDDFManager(dataSourceDescriptor, engineName) {
  override def getEngine = "aws"
}
