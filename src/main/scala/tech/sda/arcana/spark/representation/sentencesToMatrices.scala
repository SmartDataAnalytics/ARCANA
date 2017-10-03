package tech.sda.arcana.spark.representation
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.collection.mutable.ListBuffer
import com.intel.analytics.bigdl.tensor.Tensor
import shapeless._0

object sentencesToMatrices {
  //Convert a bunch of questions to bunch(table) of matrices to serve as 
  //an input for the networks
  def main(args:Array[String]){
   
  
  val sss=Tensor[Float](4,4)
  val tensorStorage= sss.storage
  
  tensorStorage.fill(99, 1, 14)
  
  
    val xxx:Array[Int]=new Array[Int](16)
    for(x<-0 to 15){
    xxx(x)=x
  }
  
  var TI:Int=0
  for(i<-0 to 10){
    tensorStorage(TI)=xxx(i)
    TI=TI+1
  }
  
  /*
    var tI=0
    var temp=0
    for(i <- 0 to xxx.size){
      temp=xxx(i)
      for(j <- 0 to temp.size-1){
        tensorStorage(tI)=temp(j).toFloat
        tI=tI+1
      }
    }
  */

  println("fuck you")
  /*
       0.0		1.0		2.0		3.0	
       4.0		5.0		6.0		7.0	
       8.0		9.0		10.0	11.0	
       12.0		13.0	14.0	15.0	
       
       
       tensorStorage.fill(0, 1, 16)
       0.0	1.0	2.0	3.0	
			4.0	5.0	6.0	7.0	
			8.0	9.0	10.0	0.0	
			0.0	0.0	0.0	0.0	
			
			0.0	1.0	2.0	3.0	
			4.0	5.0	6.0	7.0	
			8.0	9.0	10.0	0.0	
			0.0	0.0	0.0	0.0	
			
			0.0	1.0	2.0	3.0	
      4.0	5.0	6.0	7.0	
      8.0	9.0	10.0	99.0	
      99.0	99.0	0.0	0.0	

   */
  
  }
  
}