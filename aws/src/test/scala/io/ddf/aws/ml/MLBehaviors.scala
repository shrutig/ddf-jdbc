package io.ddf.aws.ml

import io.ddf.DDF
import io.ddf.jdbc.{BaseBehaviors, Loader}
import io.ddf.ml.IModel
import org.scalatest.FlatSpec

trait MLBehaviors extends BaseBehaviors {
  this: FlatSpec =>

  def ddfWithRegression(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()

    it should "do regression model computation" in {
      val model: IModel = airlineDDF.ML.train("REGRESSION")
      val prediction = airlineDDF.ML.applyModel(model)
      assert(prediction.getNumColumns > 0)
    }
  }

  def ddfWithBinary(implicit l: Loader): Unit = {
    val mtcarDDF = l.loadMtCarsDDF()

    it should "do binary model computation" in {
      val ddf: DDF = mtcarDDF
      val model: IModel = mtcarDDF.ML.train("BINARY")
      val prediction = mtcarDDF.ML.applyModel(model)
      assert(prediction.getNumColumns > 0)
    }

  }

  def ddfWithMulticlass(implicit l: Loader): Unit = {

  }

  def ddfWithCrossValidation(implicit l: Loader): Unit = {
    val airlineDDF: DDF = l.loadAirlineDDF()

    it should "do CVRandom computation" in {
      val ddf: DDF = airlineDDF
      val crossValidationRandom = ddf.ML.CVRandom(2, 0.5, 3L)
      assert(crossValidationRandom.size() == 2)
    }

    it should "do CVKFold computation" in {
      val ddf: DDF = airlineDDF
      val crossValidationRandom = ddf.ML.CVKFold(2, 3L)
      assert(crossValidationRandom.size() == 2)
    }
  }

  def ddfWithConfusionMatrix(implicit l: Loader): Unit = {
    val airlineDDF: DDF = l.loadAirlineDDF()

    it should "do confusion matrix evaluation" in {
      val ddf: DDF = airlineDDF
      val model: IModel = airlineDDF.ML.train("REGRESSION")
      val confusionMatrix = ddf.ML.getConfusionMatrix(model, 1.2)
      assert(confusionMatrix.nonEmpty)
    }
  }
}
