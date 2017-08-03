package tech.sda.arcana.spark.classification.cnn
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.intel.analytics.bigdl.utils.Engine

class BigDl_Spark {
  //check what will happen if this class was abstract
  //For the sake of building one spark session the following classes should inherent
  //this class
  def initialize(model:String):SparkContext={
   //initiate spark using the engine
   val conf = Engine.createSparkConf()
     .setAppName(model)
     .set("spark.task.maxFailures", "1")
     .setMaster("local")
   val sc = new SparkContext(conf)
   Engine.init
   return sc
  }
}