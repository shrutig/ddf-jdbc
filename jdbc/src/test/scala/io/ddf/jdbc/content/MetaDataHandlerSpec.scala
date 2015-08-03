package io.ddf.jdbc.content

import io.ddf.jdbc.BaseSpec

class MetaDataHandlerSpec extends BaseSpec {
  it should "get number of rows" in {
    val ddf = loadAirlineDDF()
    ddf.getNumRows should be(31)
  }
}
