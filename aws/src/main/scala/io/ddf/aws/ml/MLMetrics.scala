package io.ddf.aws.ml

import io.ddf.DDF
import io.ddf.aws.ml.CrossValidation
import io.ddf.content.Schema
import io.ddf.content.Schema.Column
import io.ddf.misc.{Config, ADDFFunctionalGroupHandler}
import io.ddf.ml.{MLSupporter => CoreMLSupporter, AMLMetricsSupporter, IModel, ISupportML, Model}
import java.{lang, util}

import scala.reflect.api.JavaUniverse

class MLMetrics(ddf: DDF) extends AMLMetricsSupporter(ddf) {

}