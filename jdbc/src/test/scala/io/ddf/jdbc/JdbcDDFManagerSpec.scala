package io.ddf.jdbc

class JdbcDDFManagerSpec extends BaseSpec{
  val ddf = loadAirlineDDF()
  it should "load data from file" in {
    ddf.getNamespace should be("adatao")
    ddf.getColumnNames should have size (29)

  }

  it should "be addressable via URI" in {
    ddf.getUri should be("ddf://" + ddf.getNamespace + "/" + ddf.getName)
    jdbcDDFManager.getDDFByURI("ddf://" + ddf.getNamespace + "/" + ddf.getName) should be(ddf)
  }
}
