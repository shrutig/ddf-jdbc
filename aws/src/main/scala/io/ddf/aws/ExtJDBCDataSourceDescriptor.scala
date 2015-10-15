package io.ddf.aws

import io.ddf.datasource.{DataSourceURI, JDBCDataSourceCredentials, JDBCDataSourceDescriptor}
import io.ddf.{DDF, DDFManager}

class ExtJDBCDataSourceDescriptor(uri: DataSourceURI, credentials: JDBCDataSourceCredentials, runtimeConfig:java.util.Map[String,String])
  extends JDBCDataSourceDescriptor(uri, credentials, null) {

  def getRuntimeConfiguration = runtimeConfig

  override def load(manager: DDFManager): DDF = {
    null
  }


}
