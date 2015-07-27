package io.ddf.jdbc.etl

import io.ddf.jdbc.BaseSpec

class SqlHandlerSpec extends BaseSpec {
  val airlineDDF = loadAirlineDDF()
  val yearNamesDDF = loadYearNamesDDF()

  it should "create table and load data from file" in {
    val ddf = airlineDDF
    ddf.getColumnNames should have size (29)

  }
}
