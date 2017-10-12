package tech.sda.arcana.spark.classification.cnn

import scala.collection.mutable.ListBuffer
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.utils.T
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.utils.Engine
import org.apache.spark.SparkContext
import com.intel.analytics.bigdl.nn
import org.apache.spark.sql.SQLContext
import com.intel.analytics.bigdl.numeric.NumericFloat
// for samples
import com.intel.analytics.bigdl.dataset.Sample


object BigDlReference {
  def main(args:Array[String]){

    
    
    /*
    -Dspark.master=local[1] 
    -Dspark.executor.cores=1 
    -Dspark.total.executor.cores=1 
    -Dspark.executor.memory=1g 
    -Dspark.driver.memory=1g 
    -Xmx1024m -Xms1024m
    */
    
     /*  
        val conf = Engine.createSparkConf()
         .setAppName("Train Lenet on MNIST")
         .set("spark.task.maxFailures", "1")
         .setMaster("local")
       val sc = new SparkContext(conf)
       Engine.init
    
       println(s" the Engine status is ${Engine.init}")
       println("Hello")
    */  
    println("-----------------Begin-----------------------")
    val image=Tensor[Float](3,5,5).rand
    val label=Tensor(T(1f))    
    val sample=Sample(image, label)
    println(image)
    println("------------------#1#----------------------")
    println(label)
    println("------------------#2#----------------------")
    println(sample)
    println("------------------#3#----------------------")
    println(Array(1, 28, 28))
    //test
    //val test=Tensor[Float](1,3,3).set()
    
    
    //val test=Sample(Array(1, 28, 28),Array(1, 28, 28))
    /*
     *  x = torch.Tensor(4,5)
        i = 0
        
        x:apply(function()
          i = i + 1
          return i
        end)import scala.collection.mutable.ListBuffer
     */
    
    //Tensor[Float](1,2,2).set(Array(1, 28, 28),Array(1, 28, 28),Array(1, 28, 28),Array(1, 28, 28))
    val x=Tensor[Float](2,2)
    
    val s= x.storage()
    println(s.size)
    val fi=Array(1,2,3,4)
    println("------------------#Done#----------------------")
    for(i <- 0 to s.size-1) 
      s(i) = fi(i)
    
    println("------------------#Start#----------------------")
    println(x)
    println("------------------#End#----------------------")
    
    println(Array(1, 28, 28))
    Reshape(Array(1, 28, 28))
    println("you idiote")

    val senRep:ListBuffer[Array[String]]=ListBuffer()
    senRep+=Array("1", "28", "28")
    senRep+=Array("1", "28", "28")
    senRep+=Array("1", "28", "28")
    val xx=Tensor[Float](3,3)
    val ss= xx.storage()
    //check if it can be done without new variable
    val revsenRep=senRep.reverse
    var temp:Array[String] = new Array[String](3)
    
    var h=0
    for(i <- 0 to revsenRep.size-1){
      temp=revsenRep(i)
      for(j <- 0 to temp.size-1) {
        ss(h) =temp(j).toFloat
        h=h+1
      }
    }
    
    println(xx)
    
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