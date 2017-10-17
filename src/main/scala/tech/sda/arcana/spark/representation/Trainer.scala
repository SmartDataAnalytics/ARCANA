package tech.sda.arcana.spark.representation
import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.nn.L1Cost

class Trainer(lossfun:Int,model:Int) {
  val lossFunctions = Array(L1Cost[Float](),ClassNLLCriterion[Float]())
  val models=Array()
  
  
}