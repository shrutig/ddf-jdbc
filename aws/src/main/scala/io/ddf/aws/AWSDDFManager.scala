package io.ddf.aws

import io.ddf.jdbc.JdbcDDFManager

class AWSDDFManager extends JdbcDDFManager{
  override def getEngine = "aws"
}
