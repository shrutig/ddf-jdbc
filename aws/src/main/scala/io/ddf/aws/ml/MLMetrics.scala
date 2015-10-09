package io.ddf.aws.ml

import io.ddf.DDF
import io.ddf.misc.{Config, ADDFFunctionalGroupHandler}
import io.ddf.ml.{MLSupporter => CoreMLSupporter, AMLMetricsSupporter, IModel, ISupportML, Model}


class MLMetrics(ddf: DDF) extends AMLMetricsSupporter(ddf) {

}