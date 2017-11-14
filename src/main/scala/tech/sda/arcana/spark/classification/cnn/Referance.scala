package tech.sda.arcana.spark.classification.cnn
import java.io._
import org.apache.spark.rdd.RDD
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.nn.View
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.collection.mutable.ListBuffer
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.dataset.MiniBatch
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.nn.MSECriterion
import com.intel.analytics.bigdl.utils.T
import shapeless._0
import tech.sda.arcana.spark.neuralnetwork.model.LeNet5Model
import tech.sda.arcana.spark.neuralnetwork.model.DyLeNet5Model
import com.intel.analytics.bigdl.nn.Reshape
import com.intel.analytics.bigdl.nn.Module


object Referance {
  //---------------------------------------------------------
  //Define the width and height of the tensor
  val width=10
  val height=10
  //Build any tensor manually with filling it values as desired
  val tensor=Tensor[Float](width,height)
  //The storage used to fill the real tensor
  val tensorStorage= tensor.storage()      
  //define the tensor index
  var tI=0        
  //to fill the tensor  
  for(value <- 0 to (width*height)-1){
    tensorStorage(tI)=value
    tI=tI+1
  }
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Different dimensions of tensors
  var tensorF=Tensor[Float](2,2)
  println("--------------2,2--------------------------")
  println(tensorF)//just like 2D array
  println("--------------3,3--------------------------")
  tensorF=Tensor[Float](3,3)
  println(tensorF)//just like 3D array (cube)
  println("--------------3,3,3--------------------------")
  tensorF=Tensor[Float](3,3,3)
  println(tensorF)//three layers and each layer is 3*3 (cube)
  println("--------------2,2,2--------------------------")
  tensorF=Tensor[Float](2,2,2)
  println(tensorF)//two layers and each layers is 2*2 (array)
  println("--------------4,2,3--------------------------")
  tensorF=Tensor[Float](4,2,3)
  println(tensorF)//four layers and each layers is 2*3
  println("--------------4,2,3,5--------------------------")
  tensorF=Tensor[Float](4,2,3,5)
  println(tensorF)////eight layers (4*2) and each layers is 3*5
  //the follwing printed layers
  //(1,1,.,.)...(1,2,.,.)...(2,1,.,.)...(2,2,.,.)...(3,1,.,.) 
  //(3,2,.,.)...(4,1,.,.)...(4,2,.,.)
  println("--------------4,2,3,9--------------------------")
  tensorF=Tensor[Float](4,2,3,9)
  println(tensorF)//eight layers (4*2) and each layers is 3*9
  println("--------------4,4,3,9,4--------------------------")
  tensorF=Tensor[Float](4,4,3,9,4)
  println(tensorF)//48 layers (4*4*3) and each layers is 9*4

  
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Working with Iterators
  val it = Iterator(("Mohamad",(("m",1),(1,2,3))))
  //Convert to any other shape like sequences, arrays and enter their values 
  val itToArray=it.toArray
  //Entering values and sorting depend them      
  val itToSeq=(it.toSeq).sortBy(_._2._1._1)
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //List important operations on neural networks models
  println("------------output-----------------")  
  println(LeNet5Model.build(5).output)//null do not use
  println("------------evaluate-----------------")
  println(LeNet5Model.build(5).evaluate())//draw the deep neural structure
  println("------------checkEngineType-----------------")
  println(LeNet5Model.build(5).checkEngineType())//draw the deep neural structure (same)
  println("------------isTraining-----------------")
  println(LeNet5Model.build(5).isTraining())//true or false
  println("-------------training----------------")
  println(LeNet5Model.build(5).training())//draw the deep neural structure (same)
  println("-----------getTimes------------------")
  println(LeNet5Model.build(5).getTimes())//produce time tubels
  println("-----------------------------")
  //in addition to predict for RDD samples and forward for Tensors
  //---------------------------------------------------------
}