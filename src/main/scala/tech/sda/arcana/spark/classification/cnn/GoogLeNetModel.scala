package tech.sda.arcana.spark.classification.cnn
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.utils.Engine
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object GoogLeNetModel {
  
    //initiate spark using the engine
   val conf = Engine.createSparkConf()
     .setAppName("First Convolutional network Module")
     .set("spark.task.maxFailures", "1")
     .setMaster("local")
   val sc = new SparkContext(conf)
   Engine.init
  
   
      /*
    * local cunn = nn
      local cudnn = nn
      local nClasses = 1e3
      
      -- Some shortcuts
      local SC  = cudnn.SpatialConvolution
      local SMP = cudnn.SpatialMaxPooling
      local RLU = cudnn.ReLU
    */
   
   def inc(input_size:Int,config:Array[Array[Int]]):String={
     val depthCat=Concat(2) // -- should be 1, 2 considering batches
     val conv1=Sequential()
     conv1.add(SpatialConvolution(8,config(1)(1),1,1))
     
     return "asdf"
   }
  
   /*
    * local function inc(input_size, config) -- inception
   local depthCat = nn.Concat(2) -- should be 1, 2 considering batches

   local conv1 = nn.Sequential()
   conv1:add(SC(input_size, config[1][1], 1, 1)):add(RLU(true))
   depthCat:add(conv1)

   local conv3 = nn.Sequential()
   conv3:add(SC(input_size, config[2][1], 1, 1)):add(RLU(true))
   conv3:add(SC(config[2][1], config[2][2], 3, 3, 1, 1, 1, 1)):add(RLU(true))
   depthCat:add(conv3)

   local conv5 = nn.Sequential()
   conv5:add(SC(input_size, config[3][1], 1, 1)):add(RLU(true))
   conv5:add(SC(config[3][1], config[3][2], 5, 5, 1, 1, 2, 2)):add(RLU(true))
   depthCat:add(conv5)

   local pool = nn.Sequential()
   pool:add(SMP(config[4][1], config[4][1], 1, 1, 1, 1))
   pool:add(SC(input_size, config[4][2], 1, 1)):add(RLU(true))
   depthCat:add(pool)

   return depthCat
end
    */
   
   
   
   
   
}