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
  println(tensorF)
  println("--------------3,3--------------------------")
  tensorF=Tensor[Float](3,3)
  println(tensorF)
  println("--------------3,3,3--------------------------")
  tensorF=Tensor[Float](3,3,3)
  println(tensorF)
  println("--------------2,2,2--------------------------")
  tensorF=Tensor[Float](2,2,2)
  println(tensorF)
  println("--------------4,2,3--------------------------")
  tensorF=Tensor[Float](4,2,3)
  println(tensorF)
  println("--------------4,2,3,5--------------------------")
  tensorF=Tensor[Float](4,2,3,5)
  println(tensorF)
  println("--------------4,2,3,9--------------------------")
  tensorF=Tensor[Float](4,2,3,9)
  println(tensorF)
  println("--------------4,4,3,9,4--------------------------")
  tensorF=Tensor[Float](4,4,3,9,4)
  println(tensorF)

  
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
  //---------------------------------------------------------
}