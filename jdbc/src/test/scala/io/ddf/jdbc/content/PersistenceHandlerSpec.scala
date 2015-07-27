package io.ddf.jdbc.content


import java.io.File

import io.ddf.content.APersistenceHandler.PersistenceUri
import io.ddf.{DDF, DDFManager}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class PersistenceHandlerSpec extends FlatSpec with Matchers {


  it should "hold namespaces correctly" in {
    val manager: DDFManager = DDFManager.get("jdbc")
    val ddf: DDF = manager.newDDF

    val namespaces = ddf.getPersistenceHandler.listNamespaces

    namespaces should not be (null)
    for (namespace <- namespaces.asScala) {
      val ddfs = ddf.getPersistenceHandler.listItems(namespace)
      ddfs should not be (null)
    }

  }

  it should "persist and unpersist a jdbc DDF" in {
    val manager: DDFManager = DDFManager.get("jdbc")
    val ddf: DDF = manager.newDDF
    val uri: PersistenceUri = ddf.persist
    uri.getEngine should be("jdbc")
    new File(uri.getPath).exists should be(true)
    ddf.unpersist
  }
}
