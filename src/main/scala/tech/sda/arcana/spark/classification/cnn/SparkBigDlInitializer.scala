package tech.sda.arcana.spark.classification.cnn
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.intel.analytics.bigdl.utils.Engine

/** A class to build or initialize SparkContext depending on BigDl configurations (instead of SparkConf)
 *  @constructor create a new initializer with a maxFailures and master
 *  @param maxFailures the number of maximum failures allowed
 *  @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
 */
class SparkBigDlInitializer(maxFailures: String = "1", master: String = "local[3]") {  
  val this.maxFailures:String=maxFailures
  val this.master:String=master
  
  
   /** Initialize SparkContext depending on BigDl predefined configurations
    *  @param model the neural network application name (purpose)
    *  @return SparkContext object, which tells Spark how to access
    *  a cluster 
    */
   def initialize(model:String):SparkContext={
   //initiate spark using the engine
   val conf = Engine.createSparkConf()
     .setAppName(model)
     .set("spark.task.maxFailures", maxFailures)
     .setMaster(master)
   val sc = new SparkContext(conf)
   Engine.init
   return sc
  }
  
}