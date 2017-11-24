package tech.sda.arcana.spark.classification.cnn
import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import tech.sda.arcana.spark.neuralnetwork.model.DyLeNet5Model
import com.intel.analytics.bigdl.nn.L1Cost
import tech.sda.arcana.spark.neuralnetwork.model.AlexNetModel
import tech.sda.arcana.spark.neuralnetwork.model.GoogLeNetModel
import tech.sda.arcana.spark.neuralnetwork.model.LeNet5Model
import com.intel.analytics.bigdl.optim._
import org.apache.spark.rdd.RDD
import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.convCriterion
import com.intel.analytics.bigdl.dataset.MiniBatch

/**A class that train a chosen neural network model with chosen loss function
 * @param lossfun 1 for L1Cost, 2 for ClassNLLCriterion
 * @param model 1 for AlexNetModel, 2 for GoogLeNetModel, 3 for dynamic LeNet5Model
 * @param height the longest question word sequence essential for neurons view 
 * @param width the vectors representation length for each word
 * @param classNum the number of ouptut classes
 */
class Trainer(lossfun:Int,model:Int,height:Int,width:Int,classNum:Int) extends Serializable  {
  val lossFunctions = Array(L1Cost[Float](),ClassNLLCriterion[Float]())
 
  /** Build a trainer which is going to train the a neural network model
   *  depending on a training set and a batch size
   *  @param samples 1 for L1Cost, 2 for ClassNLLCriterion
   *  @param batch 1 for L1Cost, 2 for ClassNLLCriterion
   */ 
  def build(samples:RDD[Sample[Float]],batch:Int):Optimizer[Float, MiniBatch[Float]]={
   //There is being repetition in the code because of using singleton objects in Scala
   //Abstract class didn't solve the problem because the return
   //type is going to be from the parent class
   //Switch case didn't solve the problem because the return time is object
    if(model==1){
         val optimizer = Optimizer(
          model = AlexNetModel.build(),
          sampleRDD = samples,
          criterion = lossFunctions(lossfun-1),
          batchSize = batch
            )
    return optimizer
            //(optimizer)
   }
   
   if(model==2){
         val optimizer = Optimizer(
          model = GoogLeNetModel.build(),
          sampleRDD = samples,
          criterion = lossFunctions(lossfun-1),
          batchSize = batch
            )
     return optimizer
          //(optimizer)
   }
   
   if(model==3){
         val optimizer = Optimizer(
          model = DyLeNet5Model.build(height,width,classNum),
          sampleRDD = samples,
          criterion = lossFunctions(lossfun-1),
          batchSize = batch
            )
     return optimizer
           //(optimizer)
   }
  
   // The general case to know the return type
      val optimizer = Optimizer(
      model = LeNet5Model.build(5),
      sampleRDD = samples,
      criterion = lossFunctions(lossfun-1),
      batchSize = batch
          )
     return optimizer
          //(optimizer)

  }
   
}