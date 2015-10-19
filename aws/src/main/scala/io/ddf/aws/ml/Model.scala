package io.ddf.aws.ml

import com.amazonaws.services.machinelearning.model.MLModelType
import io.ddf.content.Schema


case class Model(awsModel: AwsModel) extends io.ddf.ml.Model(awsModel)


class AwsModel(awsMLHelper: AwsMLHelper, schema: Schema, modelId: String, mlModelType: MLModelType) {

  def getModelId = modelId

  def getMLModelType = mlModelType

  def getSchema = schema
}

case class BinaryClassification(awsMLHelper: AwsMLHelper, schema: Schema, modelId: String, mlModelType: MLModelType) extends AwsModel(awsMLHelper, schema, modelId, mlModelType) {
  def predict(row: Seq[_]): String = awsMLHelper.predict(row, this).right.get
}

case class MultiClassClassification(awsMLHelper: AwsMLHelper, schema: Schema, modelId: String, mlModelType: MLModelType) extends AwsModel(awsMLHelper, schema, modelId, mlModelType) {
  def predict(row: Seq[_]): String = awsMLHelper.predict(row, this).right.get
}

case class LinearRegression(awsMLHelper: AwsMLHelper, schema: Schema, modelId: String, mlModelType: MLModelType) extends AwsModel(awsMLHelper, schema, modelId, mlModelType) {
  def predict(row: Seq[_]): Float = awsMLHelper.predict(row, this).left.get
}
