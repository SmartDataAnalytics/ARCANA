package tech.sda.arcana.spark.neuralnetwork.model
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._

/** Object represents AlexNetModel */
object AlexNetModel {
  
  /** creates an instance of AlexNetModel model */
  def build(Height:Int,Width:Int,classNum: Int)={
  val firstbranch=Sequential()
  //Adding a padding layer
  firstbranch.add(SpatialZeroPadding(0, 224-Width, 0, 224-Height))
  //val spatialZeroPadding = SpatialZeroPadding(0, 224-Width, 0, 224-Height)
  //Achtung 3->1 from me
  firstbranch.add(SpatialConvolution(nInputPlane=1,nOutputPlane=48,kernelW=11,kernelH=11,strideW=4,strideH=4,padW=2,padH=2)) //-- 224 -> 55
  //Rectified Linear Unit non-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())// maybe needs a true here as an argument
  //kW=kernel width,kH=kernel height,dW=step size in width,dH=step size in height
  firstbranch.add(SpatialMaxPooling(kW=3,kH=3,dW=2,dH=2)) //55 ->  27
  firstbranch.add(SpatialConvolution(48,128,5,5,1,1,2,2)) //27 -> 27
  //Rectified Linear Unit non-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())
  firstbranch.add(SpatialMaxPooling(3,3,2,2)) //27 ->  13
  firstbranch.add(SpatialConvolution(128,192,3,3,1,1,1,1)) //13 -> 13
  //Rectified Linear Unit non-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())
  firstbranch.add(SpatialConvolution(192,192,3,3,1,1,1,1)) //13 -> 13
  //Rectified Linear Unit non-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())
  firstbranch.add(SpatialConvolution(192,128,3,3,1,1,1,1)) //13 -> 13
  //Rectified Linear Unit noRectified Linear Unitn-linearity (+Z) =max(0,x)
  firstbranch.add(ReLU())
  firstbranch.add(SpatialMaxPooling(3,3,2,2)) //13 ->  6
  
  val secondbranch=Sequential()
  //Adding a padding layer
  secondbranch.add(SpatialZeroPadding(0, 224-Width, 0, 224-Height))
  //val spatialZeroPadding = SpatialZeroPadding(0, 224-Width, 0, 224-Height)
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
  
  def graph(Height:Int,Width:Int,classNum: Int)={
  //FB first branch
  //SB second branch
   
  val padding1=SpatialZeroPadding(0, 224-Width, 0, 224-Height).inputs()
  val conv1FB = SpatialConvolution(1,48,11,11,4,4,2,2).inputs(padding1)
  val recLinUn1FB = ReLU().inputs(conv1FB)
  val pool1FB = SpatialMaxPooling(kW=3,kH=3,dW=2,dH=2).inputs(recLinUn1FB)
  val conv2FB = SpatialConvolution(48,128,5,5,1,1,2,2).inputs(pool1FB)
  val recLinUn2FB = ReLU().inputs(conv2FB)
  val pool2FB = SpatialMaxPooling(3,3,2,2).inputs(recLinUn2FB)
  val conv3FB = SpatialConvolution(128,192,3,3,1,1,1,1).inputs(pool2FB)
  val recLinUn3FB = ReLU().inputs(conv3FB)
  val conv4FB = SpatialConvolution(192,192,3,3,1,1,1,1).inputs(recLinUn3FB)
  val recLinUn4FB=ReLU().inputs(conv4FB)
  val conv5FB = SpatialConvolution(192,128,3,3,1,1,1,1).inputs(recLinUn3FB)
  val recLinUn5FB=ReLU().inputs(conv5FB)
  val pool3FB = SpatialMaxPooling(3,3,2,2).inputs(recLinUn5FB)
  
  val padding2=SpatialZeroPadding(0, 224-Width, 0, 224-Height).inputs()
  val conv1SB = SpatialConvolution(1,48,11,11,4,4,2,2).inputs(padding2)
  val recLinUn1SB =ReLU().inputs(conv1SB)
  val pool1SB = SpatialMaxPooling(kW=3,kH=3,dW=2,dH=2).inputs(recLinUn1SB)
  val conv2SB = SpatialConvolution(48,128,5,5,1,1,2,2).inputs(pool1SB)
  val recLinUn2SB = ReLU().inputs(conv2SB)
  val pool2SB = SpatialMaxPooling(3,3,2,2).inputs(recLinUn2SB)
  val conv3SB = SpatialConvolution(128,192,3,3,1,1,1,1).inputs(pool2SB)
  val recLinUn3SB = ReLU().inputs(conv3SB)
  val conv4SB = SpatialConvolution(192,192,3,3,1,1,1,1).inputs(recLinUn3SB)
  val recLinUn4SB = ReLU().inputs(conv4SB)
  val conv5SB = SpatialConvolution(192,128,3,3,1,1,1,1).inputs(recLinUn3SB)
  val recLinUn5SB=ReLU().inputs(conv5SB)
  val pool3SB = SpatialMaxPooling(3,3,2,2).inputs(recLinUn5SB)
  
  val connector=Concat(2).inputs(pool3FB, pool3SB)
  
  val classifer=View(256*6*6).inputs(connector)
  val dropout1=Dropout(0.5).inputs(classifer)
  val linear1=Linear(256*6*6,4096).inputs(dropout1)
  val thresh1=Threshold(0,1e-6).inputs(linear1)
  val dropout2=Dropout(0.5).inputs(thresh1)
  val linear2=Linear(4096,4096).inputs(dropout2)
  val thresh2=Threshold(0,1e-6).inputs(linear2)
  val linear3=Linear(4096,classNum).inputs(thresh2)
  val logsoft=LogSoftMax().inputs(linear3)
  }
}
