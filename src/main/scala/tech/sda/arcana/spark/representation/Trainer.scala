package tech.sda.arcana.spark.representation
import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.nn.L1Cost
import tech.sda.arcana.spark.neuralnetwork.model.AlexNetModel
import tech.sda.arcana.spark.neuralnetwork.model.GoogLeNetModel
import tech.sda.arcana.spark.neuralnetwork.model.LeNet5Model
import com.intel.analytics.bigdl.optim._
import org.apache.spark.rdd.RDD
import com.intel.analytics.bigdl.dataset.Sample

/**A class that train a chosen neural network model with chosen loss function
 * @param lossfun 1 for L1Cost, 2 for ClassNLLCriterion
 * @param model 1 for AlexNetModel, 2 for GoogLeNetModel, 3 for LeNet5Model
 */
class Trainer(lossfun:Int,model:Int) {
  val lossFunctions = Array(L1Cost[Float](),ClassNLLCriterion[Float]())
  
  def build(samples:RDD[Sample[Float]],batch:Int)={

    if(model==1){
         val optimizer = Optimizer(
          model = AlexNetModel.build(),
          sampleRDD = samples,
          criterion = lossFunctions(lossfun-1),
          batchSize = batch
            )
    (optimizer)
   }
   
   if(model==2){
         val optimizer = Optimizer(
          model = GoogLeNetModel.build(),
          sampleRDD = samples,
          criterion = lossFunctions(lossfun-1),
          batchSize = batch
            )
     (optimizer)
   }
   
   if(model==3){
         val optimizer = Optimizer(
          model = LeNet5Model.build(),
          sampleRDD = samples,
          criterion = lossFunctions(lossfun-1),
          batchSize = batch
            )
     (optimizer)
   }
   
      val optimizer = Optimizer(
      model = LeNet5Model.build(),
      sampleRDD = samples,
      criterion = lossFunctions(lossfun-1),
      batchSize = batch
          )
     (optimizer)
   
  }
   
}