package tech.sda.arcana.spark.neuralnetwork.model
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._

/** Object represents AlexNetModel */
object AlexNetModel {
  
  /** creates an instance of AlexNetModel model */
  def build(classNum: Int)={
  val firstbranch=Sequential()
  //Achtung 3->1 from me
  firstbranch.add(SpatialConvolution(nInputPlane=1,nOutputPlane=48,kernelW=11,kernelH=11,strideW=4,strideH=4,padW=2,padH=2)) //-- 224 -> 55
  //non-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())// maybe needs a true here as an argument
  //kW=kernel width,kH=kernel height,dW=step size in width,dH=step size in height
  firstbranch.add(SpatialMaxPooling(kW=3,kH=3,dW=2,dH=2)) //55 ->  27
  firstbranch.add(SpatialConvolution(48,128,5,5,1,1,2,2)) //27 -> 27
  //non-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())
  firstbranch.add(SpatialMaxPooling(3,3,2,2)) //27 ->  13
  firstbranch.add(SpatialConvolution(128,192,3,3,1,1,1,1)) //13 -> 13
  //non-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())
  firstbranch.add(SpatialConvolution(192,192,3,3,1,1,1,1)) //13 -> 13
  //non-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())
  firstbranch.add(SpatialConvolution(192,128,3,3,1,1,1,1)) //13 -> 13
  //non-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())
  firstbranch.add(SpatialMaxPooling(3,3,2,2)) //13 ->  6
  
  val secondbranch=Sequential()
  //Achtung 3->1 from me
  secondbranch.add(SpatialConvolution(1,48,11,11,4,4,2,2)) //-- 224 -> 55
  //non-linearity (+Z) =max(0,x)
  secondbranch.add(ReLU())// maybe needs a true here as an argument
  secondbranch.add(SpatialMaxPooling(3,3,2,2)) //55 ->  27
  secondbranch.add(SpatialConvolution(48,128,5,5,1,1,2,2)) //27 -> 27
  //non-linearity (+Z) =max(0,x)
  secondbranch.add(ReLU())
  secondbranch.add(SpatialMaxPooling(3,3,2,2)) //27 ->  13
  secondbranch.add(SpatialConvolution(128,192,3,3,1,1,1,1)) //13 -> 13
  //non-linearity (+Z) =max(0,x)
  secondbranch.add(ReLU())
  secondbranch.add(SpatialConvolution(192,192,3,3,1,1,1,1)) //13 -> 13
  //non-linearity (+Z) =max(0,x)
  secondbranch.add(ReLU())
  secondbranch.add(SpatialConvolution(192,128,3,3,1,1,1,1)) //13 -> 13
  //non-linearity (+Z) =max(0,x)
  secondbranch.add(ReLU())
  secondbranch.add(SpatialMaxPooling(3,3,2,2)) //13 ->  6
  
  //I rewrote the previous branch because I am not sure 
  //if the following two lines of code will do the same 
  //(the documentation does not state that clearly)
  //val secondbranch=firstbranch.cloneModule()
  //secondbranch.reset()
  
  val features=Concat(2)
  features.add(firstbranch)
  features.add(secondbranch)
  
  val classifier=Sequential()
  classifier.add(View(256*6*6))
  classifier.add(Dropout(0.5))
  classifier.add(Linear(256*6*6,4096))
  classifier.add(Threshold(0,1e-6))
  classifier.add(Dropout(0.5))
  classifier.add(Linear(4096,4096))
  classifier.add(Threshold(0,1e-6))
  classifier.add(Linear(4096,classNum))
  classifier.add(LogSoftMax())
  val model=Sequential().add(features).add(classifier)
  model
  }
}
