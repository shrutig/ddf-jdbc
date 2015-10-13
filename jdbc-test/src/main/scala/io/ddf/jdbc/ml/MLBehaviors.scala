package io.ddf.jdbc.ml

import io.ddf.DDF
import io.ddf.jdbc.{BaseBehaviors, Loader}
import io.ddf.ml.IModel
import org.scalatest.FlatSpec
import io.ddf.aws.ml.MLModel

trait MLBehaviors extends BaseBehaviors {
  this: FlatSpec =>

  def ddfWithRegression(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()

    it should "do regression model computation" in{
      val ddf: DDF = airlineDDF
      val model:IModel = airlineDDF.ML.train("REGRESSION")
      val prediction = airlineDDF.ML.applyModel(model)
      assert(prediction.getNumColumns > 0 )
    }
  }

  def ddfWithBinary(implicit l:Loader):Unit = {


  }

  def ddfWithClassification(implicit l:Loader):Unit ={

  }
}
