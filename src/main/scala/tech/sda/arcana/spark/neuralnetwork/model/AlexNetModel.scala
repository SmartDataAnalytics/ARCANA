package tech.sda.arcana.spark.neuralnetwork.model
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._

/** Object represents AlexNetModel */
object AlexNetModel {
  
  /** creates an instance of AlexNetModel model */
  def build()={
  val firstbranch=Sequential()
  //Achtung 3->1 from me
  firstbranch.add(SpatialConvolution(1,48,11,11,4,4,2,2)) //-- 224 -> 55
  firstbranch.add(ReLU())// maybe needs a true here as an argument
  firstbranch.add(SpatialMaxPooling(3,3,2,2)) //55 ->  27
  firstbranch.add(SpatialConvolution(48,128,5,5,1,1,2,2)) //27 -> 27
  firstbranch.add(ReLU())
  firstbranch.add(SpatialMaxPooling(3,3,2,2)) //27 ->  13
  firstbranch.add(SpatialConvolution(128,192,3,3,1,1,1,1)) //13 -> 13
  firstbranch.add(ReLU())
  firstbranch.add(SpatialConvolution(192,192,3,3,1,1,1,1)) //13 -> 13
  firstbranch.add(ReLU())
  firstbranch.add(SpatialConvolution(192,128,3,3,1,1,1,1)) //13 -> 13
  firstbranch.add(ReLU())
  firstbranch.add(SpatialMaxPooling(3,3,2,2)) //13 ->  6
  val secondbranch=firstbranch.cloneModule()
  secondbranch.reset()
  
  /*
   * 	local fb2 = fb1:clone() -- branch 2
			for k,v in ipairs(fb2:findModules('nn.SpatialConvolution')) do
   			v:reset() -- reset branch 2's weights
			end
   */
  
  val features=Concat(2)
  features.add(firstbranch)
  features.add(secondbranch)
  
  //something strange
  val classifier=Sequential()
  classifier.add(View(256*6*6))
  classifier.add(Dropout(0.5))
  classifier.add(Linear(256*6*6,4096))
  classifier.add(Threshold(0,1e-6))
  classifier.add(Dropout(0.5))
  classifier.add(Linear(4096,4096))
  classifier.add(Threshold(0,1e-6))
  classifier.add(Linear(4096,1000))
  classifier.add(LogSoftMax())
  val model=Sequential().add(features).add(classifier)
  model
  }
}
