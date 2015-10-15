package io.ddf.aws.ml

import com.amazonaws.services.machinelearning.model.MLModelType
import io.ddf.ml.Model

class AwsModel(modelId: String, mlModelType: MLModelType) extends Model(modelId) {

  def getModelId = modelId

  def getMLModelType = mlModelType
}
