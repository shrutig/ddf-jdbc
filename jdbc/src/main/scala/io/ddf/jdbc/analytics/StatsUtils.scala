package io.ddf.jdbc.analytics

import java.{lang, util}

import com.clearspring.analytics.stream.quantile.QDigest

object StatsUtils {

  object Quantiles {

    def getQuantiles(dataSet: Seq[Double], percentiles: Array[lang.Double]): Array[lang.Double] = {
      val qDigest = new QDigest(100)
      dataSet.foreach { i =>
        qDigest.offer(i.toLong)
      }
      percentiles.map {
        i =>
          val quantile: lang.Double = qDigest.getQuantile(i).toDouble
          quantile
      }

    }
  }

  case class PearsonCorrelation(x: Double,
                                y: Double,
                                xy: Double,
                                x2: Double,
                                y2: Double,
                                count: Int) extends Serializable {

    def merge(other: PearsonCorrelation): PearsonCorrelation = {
      PearsonCorrelation(x + other.x,
        y + other.y,
        xy + other.xy,
        x2 + other.x2,
        y2 + other.y2,
        count + count
      )
    }

    def evaluate: Double = {
      val numr: Double = (count * xy) - (x * y)
      val base: Double = ((count * x2) - (x * x)) * ((count * y2) - (y * y))
      val denr: Double = Math.sqrt(base)
      val result: Double = numr / denr
      result
    }

  }

  class CovarianceCounter {
    var xAvg = 0.0
    var yAvg = 0.0
    var Ck = 0.0
    var count = 0L

    // add an example to the calculation
    def add(x: Double, y: Double): this.type = {
      val oldX = xAvg
      count += 1
      xAvg += (x - xAvg) / count
      yAvg += (y - yAvg) / count
      Ck += (y - yAvg) * (x - oldX)
      this
    }

    // merge counters from other partitions. Formula can be found at:
    // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance
    def merge(other: CovarianceCounter) = {
      val totalCount = count + other.count
      Ck += other.Ck +
        (xAvg - other.xAvg) * (yAvg - other.yAvg) * count / totalCount * other.count
      xAvg = (xAvg * count + other.xAvg * other.count) / totalCount
      yAvg = (yAvg * count + other.yAvg * other.count) / totalCount
      count = totalCount
      this
    }

    // return the sample covariance for the observed examples
    def cov: Double = Ck / (count - 1)

    def reset(): Unit = {
      xAvg = 0.0
      yAvg = 0.0
      Ck = 0.0
      count = 0L
    }

    def add(v: (Double, Double)): Unit = add(v._1, v._2)
  }

  class Histogram(binKeys: Seq[Double]) {

    private val treeMap: util.TreeMap[Double, Integer] = new util.TreeMap[Double, Integer]
    binKeys.foreach(treeMap.put(_, null))


    def add(value: Double) {
      var theKey: Double = this.treeMap.floorKey(value)
      theKey = if (theKey == null) value else theKey
      val current: Integer = this.treeMap.get(theKey)
      val toAdd = (if (current != null) current.intValue() else 0)
      val newValue: Integer = toAdd+ 1
      this.treeMap.put(theKey, newValue)
    }

    def getValue: util.TreeMap[Double, Integer] = {
      this.treeMap
    }

    def merge(other: Histogram) ={
      import scala.collection.JavaConversions._
      for (entryFromOther <- other.getValue.entrySet) {
        val ownValue: Integer = this.treeMap.get(entryFromOther.getKey)
        if (ownValue == null) {
          this.treeMap.put(entryFromOther.getKey, entryFromOther.getValue)
        }
        else {
          this.treeMap.put(entryFromOther.getKey, entryFromOther.getValue + ownValue)
        }
      }
      this
    }

    def reset {
      this.treeMap.clear
    }

    override def toString: String = {
      return this.treeMap.toString
    }
  }

}
