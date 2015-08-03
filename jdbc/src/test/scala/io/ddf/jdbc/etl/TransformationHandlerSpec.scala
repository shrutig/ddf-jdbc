package io.ddf.jdbc.etl

import com.google.common.collect.Lists
import io.ddf.DDF
import io.ddf.analytics.Summary
import io.ddf.etl.TransformationHandler
import io.ddf.jdbc.BaseSpec

class TransformationHandlerSpec extends BaseSpec{

  val ddf = loadAirlineDDF().sql2ddf("select year, month, deptime, arrtime, distance, arrdelay, depdelay from airline")

  it should "transform scale min max" in {
    ddf.getSummary foreach println _

    val newddf0: DDF = ddf.Transform.transformScaleMinMax

    val summaryArr: Array[Summary] = newddf0.getSummary
    println("result summary is"+summaryArr(0))
    summaryArr(0).min should be < 1.0
    summaryArr(0).max should be(1.0)
  }

  it should "transform scale standard" in {
    val newDDF: DDF = ddf.Transform.transformScaleStandard()
    newDDF.getNumRows should be (31)
    newDDF.getSummary.length should be (7)
  }


  it should "test transform udf" in{
    val newddf = ddf.Transform.transformUDF("dist= round(distance/2, 2)")
    newddf.getNumRows should be (31)
    newddf.getNumColumns should be (8)
    newddf.getColumnName(7).toLowerCase should be ("dist")

    val newddf2 = newddf.Transform.transformUDF("arrtime-deptime")
    newddf2.getNumRows should be (31)
    newddf2.getNumColumns should be (9)

    val cols = Lists.newArrayList("distance", "arrtime", "deptime", "arrdelay")
    val newddf3 = newddf2.Transform.transformUDF("speed = distance/(arrtime-deptime)", cols)
    newddf3.getNumRows should be (31)
    newddf3.getNumColumns should be (5)
    newddf3.getColumnName(4).toLowerCase should be ("speed")

    val newddf4 = newddf3.Transform.transformUDF("arrtime-deptime,(speed^*- = distance/(arrtime-deptime)", cols)
    newddf4.getNumRows should be (31)
    newddf4.getNumColumns should be (6)
    newddf4.getColumnName(5).toLowerCase should be ("speed")

    val lcols= Lists.newArrayList("distance", "arrtime", "deptime")
    val s0: String = "new_col = if(arrdelay=15,1,0)"
    val s1: String = "new_col = if(arrdelay=15,1,0),v ~ (arrtime-deptime),distance/(arrtime-deptime)"
    val s2: String = "arr_delayed=if(arrdelay=\"yes\",1,0)"
    val s3: String = "origin_sfo = case origin when \'SFO\' then 1 else 0 end "
    val res1 = "(if(arrdelay=15,1,0)) as new_col,((arrtime-deptime)) as v,(distance/(arrtime-deptime))"
    val res2 = "(if(arrdelay=\"yes\",1,0)) as arr_delayed"
    val res3 = "(case origin when \'SFO\' then 1 else 0 end) as origin_sfo"
    TransformationHandler.RToSqlUdf(s1) should be (res1)
    TransformationHandler.RToSqlUdf(s2) should be (res2)
    TransformationHandler.RToSqlUdf(s3) should be (res3)

  }


}
