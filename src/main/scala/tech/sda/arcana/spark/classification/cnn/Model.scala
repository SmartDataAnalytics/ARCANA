package tech.sda.arcana.spark.classification.cnn
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.utils.Engine
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/*
 * Convolutional Neural Networks main model.
 */
object Model {
  
  //initiate spark using the engine
   val conf = Engine.createSparkConf()
     .setAppName("First Convolutional network Module")
     .set("spark.task.maxFailures", "1")
     .setMaster("local")
   val sc = new SparkContext(conf)
   Engine.init
  
  val testModel= new Sequential()
  // 1 input channel, 6 output channels, 5x5 convolution kernel
  testModel.add(SpatialConvolution(1, 6, 5, 5))
  //non-linearity (+Z)
  testModel.add(ReLU())
  //A max-pooling operation that looks at 2x2 windows and finds the max.
  testModel.add(SpatialMaxPooling(2,2,2,2))
  testModel.add(SpatialConvolution(6,16,5,5))
  testModel.add(ReLU())
  testModel.add(SpatialMaxPooling(2,2,2,2))
  //reshapes from a 3D tensor of 16x5x5 into 1D tensor of 16*5*5
  testModel.add(View(16*5*5))
  //fully connected layer (matrix multiplication between input and weights)
  testModel.add(Linear(16*5*5,120))
  testModel.add(ReLU())
  testModel.add(Linear(120,84))
  testModel.add(ReLU())
  testModel.add(Linear(84,10))
  testModel.add(LogSoftMax())
    
  
}