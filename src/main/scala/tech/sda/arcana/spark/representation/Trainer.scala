package tech.sda.arcana.spark.representation
import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.nn.L1Cost
import tech.sda.arcana.spark.neuralnetwork.model.AlexNetModel
import tech.sda.arcana.spark.neuralnetwork.model.GoogLeNetModel
import tech.sda.arcana.spark.neuralnetwork.model.LeNet5Model

class Trainer(lossfun:Int,model:Int) {
  val lossFunctions = Array(L1Cost[Float](),ClassNLLCriterion[Float]())
  val models=Array()
  
  
}