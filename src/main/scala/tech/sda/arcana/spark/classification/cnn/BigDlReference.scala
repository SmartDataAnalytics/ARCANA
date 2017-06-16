package tech.sda.arcana.spark.classification.cnn
/*
//1
import com.intel.analytics.bigdl.tensor.Tensor
//2
import com.intel.analytics.bigdl.utils.T
//3
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._
*/
/*
import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.utils.Engine

import org.apache.spark.SparkContext
*/
import com.intel.analytics.bigdl.nn
import com.intel.analytics.bigdl.utils.Engine
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import com.intel.analytics.bigdl.numeric.NumericFloat

import com.intel.analytics.bigdl.nn._

import com.intel.analytics.bigdl._

object BigDlReference {
  def main(args:Array[String]){

    
    val conf = Engine.createSparkConf()
     .setAppName("Train Lenet on MNIST")
     .set("spark.task.maxFailures", "1")
     .setMaster("local")
   val sc = new SparkContext(conf)
   Engine.init

   println(s" the Engine status is ${Engine.init}")
   println("Hello")

    
    
    
/*
   val conf = Engine.createSparkConf()
   val sc = new SparkContext(conf)
   Engine.init
    */
    
    
    /*
  //1
  val tensor = Tensor[Float](2, 3)
  println(tensor)
  //2
  val table= T(Tensor[Float](2,2), Tensor[Float](2,2))
  println(table)
  //3
  val f = Linear(3,4)
  val weights=f.weight
  println(weights)
  * */
  }
} 