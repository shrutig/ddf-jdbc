package io.ddf.jdbc.analytics

import com.google.common.collect.Lists
import io.ddf.jdbc.BaseSpec
import io.ddf.types.AggregateTypes.AggregateFunction

class AggregationHandlerSpec extends BaseSpec {
  val ddf = loadAirlineDDF().sql2ddf("select year, month, dayofweek, deptime, arrtime,origin, distance, arrdelay, depdelay, carrierdelay, weatherdelay, nasdelay, securitydelay, lateaircraftdelay from airline")

  it should "calculate simple aggregates" in {
    val res1 = ddf.aggregate("year, month, min(depdelay), max(arrdelay)")
    res1.size() should be (13)
    val aggregateResult = ddf.aggregate("year, month, avg(depdelay), stddev(arrdelay)")
    val result: Array[Double] = aggregateResult.get("2010,3")
    result.length should be(2)

    val colAggregate = ddf.getAggregationHandler.aggregateOnColumn(AggregateFunction.MAX, "YEAR")
    colAggregate should be(2010)
  }

  it should "group data" in {
    val l1 = Lists.newArrayList("year", "month")
    val l2 = Lists.newArrayList("m=avg(depdelay)")
    val l3 = Lists.newArrayList("m= stddev(arrdelay)")
    val l4 = Lists.newArrayList("dayofweek")
    ddf.groupBy(l1, l2).getNumRows should be (13)
    ddf.groupBy(l4, l3).getNumRows should be (1)

    ddf.groupBy(l1).agg(l2).getNumRows() should be (13)
    ddf.groupBy(Lists.newArrayList("origin")).agg(Lists.newArrayList("metrics = count(*)")).getNumRows() should be (3)
    ddf.groupBy(Lists.newArrayList("origin")).agg(Lists.newArrayList("metrics = count(1)")).getNumRows() should be (3)
    ddf.groupBy(Lists.newArrayList("origin")).agg(Lists.newArrayList("metrics = count(dayofweek)")).getNumRows() should be (3)
    ddf.groupBy(Lists.newArrayList("origin")).agg(Lists.newArrayList("metrics = avg(arrdelay)")).getNumRows() should be (3)
  }

  it should "calculate correlation" in {
    //0.8977184691827954
    ddf.correlation("depdelay", "arrdelay") should be (0.89 +- 1)
  }
}
