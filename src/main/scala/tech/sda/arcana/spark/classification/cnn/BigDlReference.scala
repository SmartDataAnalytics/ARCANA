/*package tech.sda.arcana.spark.classification.cnn
//1
import com.intel.analytics.bigdl.tensor.Tensor
//2
import com.intel.analytics.bigdl.utils.T
//3
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._

object BigDlReference {
  def main(args:Array[String]){
  
    
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
  }
}*/ 