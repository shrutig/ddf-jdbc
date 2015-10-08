package io.ddf.jdbc.ml

import io.ddf.jdbc.{Loader, BaseBehaviors}
import org.scalatest.FlatSpec

trait MLBehaviors extends BaseBehaviors {
  this: FlatSpec =>

  def ddfWithRegression(implicit l: Loader): Unit = {
    val airlineDDF = l.loadAirlineDDF()
    val yearNamesDDF = l.loadYearNamesDDF()

  }

  def ddfWithBinary(implicit l:Loader):Unit = {

  }

  def ddfWithClassification(implicit l:Loader):Unit ={

  }
}