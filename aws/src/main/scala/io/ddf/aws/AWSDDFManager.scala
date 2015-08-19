package io.ddf.aws

import io.ddf.postgres.PostgresDDFManager

class AWSDDFManager extends PostgresDDFManager {
  override def getEngine = "aws"
}
