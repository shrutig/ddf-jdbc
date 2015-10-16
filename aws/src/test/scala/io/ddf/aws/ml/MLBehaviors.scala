package io.ddf.aws.ml

import java.util

import io.ddf.DDF
import io.ddf.aws.AWSLoader
import io.ddf.jdbc.{BaseBehaviors, Loader}
import io.ddf.ml.IModel
import org.scalatest.FlatSpec

trait MLBehaviors extends BaseBehaviors {
  this: FlatSpec =>

  def ddfWithRegression(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()

    it should "do regression model computation" in {
      val ddf: DDF = airlineDDF
      val model: IModel = ddf.ML.train("REGRESSION")
      val prediction = ddf.ML.applyModel(model)
      assert(prediction.getNumColumns > 0)
    }
  }

  def ddfWithModelParameters(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()

    it should "do regression model computation with parameters" in {
      val ddf: DDF = airlineDDF
      val map:java.util.Map[String,String] = new java.util.HashMap[String,String]()
      map.put("sgd.l1RegularizationAmount","1.0E-08")
      map.put("sgd.l2RegularizationAmount","1.0E-08")
      map.put("sgd.maxPasses","10")
      map.put("sgd.maxMLModelSizeInBytes","33554432")
      val model: IModel = ddf.ML.train("REGRESSION",map)
      val prediction = ddf.ML.applyModel(model)
      assert(prediction.getNumColumns > 0)
    }
  }

  def ddfWithBinary(implicit l: Loader): Unit = {
    val mtcarDDF = l.loadMtCarsDDF()

    it should "do binary model computation" in {
      val ddf: DDF = mtcarDDF.sql2ddf("SELECT mpg ,cyl , disp , hp, drat , wt, qsec, vs FROM ddf://adatao/mtcars")
      val model: IModel = ddf.ML.train("BINARY")
      val prediction = ddf.ML.applyModel(model)
      assert(prediction.getNumColumns > 0)
    }

  }

  def ddfWithMulticlass(implicit l: Loader): Unit = {

    val mtcarDDF = l.loadMtCarsDDF()

    it should "do multiclass model computation" in {
      val ddf: DDF = mtcarDDF
      val model: IModel = ddf.ML.train("MULTICLASS")
      val prediction = ddf.ML.applyModel(model)
      assert(prediction.getNumColumns > 0)
    }
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
      val model: IModel = ddf.ML.train("REGRESSION")
      val confusionMatrix = ddf.ML.getConfusionMatrix(model, 1.2)
      assert(confusionMatrix.nonEmpty)
    }
  }

  def ddfWithMetrics(implicit l: Loader): Unit = {
    val airlineDDF: DDF = l.loadAirlineDDF()

    it should "do rmse evaluation" in {
      val ddf: DDF = airlineDDF
      val rmse = ddf.getMLMetricsSupporter.rmse(ddf,true)
      assert(rmse > 0)
    }

    it should "do roc computation" in{
      val ddf: DDF = airlineDDF
      val rocMetric = ddf.getMLMetricsSupporter.roc(ddf,1)
      assert(rocMetric.auc >0)
    }
  }

}
