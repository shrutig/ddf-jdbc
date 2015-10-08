
package io.ddf.aws.ml

import com.amazonaws.services.machinelearning.model.MLModelType
import io.ddf.DDF
import io.ddf.jdbc.JdbcDDF
import io.ddf.misc.{ADDFFunctionalGroupHandler, Config}
import io.ddf.ml.{ CrossValidationSet, IModel, ISupportML, Model}
import java.{util,lang}
import scala.reflect.runtime.{universe => ru}


class MLSupport(ddf: DDF) extends  ADDFFunctionalGroupHandler(ddf) with ISupportML with Serializable {

  override def train(trainMethodKey: String, args: AnyRef*): IModel = {
    val sql = "SELECT * FROM " + ddf.getTableName
    val datasourceId = AwsModelHelper.createDataSourceFromRedShift(sql)
    val modelId = AwsModelHelper.createModel(datasourceId, Config.getValue(ddf.getEngine, "recipe"), MLModelType.valueOf
      (trainMethodKey),
      args.asInstanceOf[java.util.Map[String, String]])
    new MLModel(modelId)
  }

  override def applyModel(model: IModel): DDF = applyModel(model, true)

  override def applyModel(model: IModel, hasLabels: Boolean): DDF = applyModel(model, hasLabels, true)

  override def applyModel(model: IModel, hasLabels: Boolean, includeFeatures: Boolean): DDF = {
    val awsModel = model.asInstanceOf[MLModel]
    val sql = "SELECT * FROM " + ddf.getTableName
    val datasourceId = AwsModelHelper.createDataSourceFromRedShift(sql)
    awsModel.predictDataSource(ddf,datasourceId)
  }

 //TODO: getConfusionMatrix implementation
  val awsddf = ddf.asInstanceOf[JdbcDDF]
  val crossValidation: CrossValidation = new CrossValidation(awsddf)

  def CVRandom(k:Int, trainingSize: Double, seed: lang.Long): util.List[CrossValidationSet] = {
    crossValidation.CVRandom(k, trainingSize, seed)
  }

  def CVKFold(k: Int, seed: lang.Long): util.List[CrossValidationSet] = {
    crossValidation.CVK(k, seed)
  }

}