package tech.sda.arcana.spark.neuralnetwork.model
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._

/** FObject represents GoogleNetModel */
object GoogLeNetModel {
  
  /** creates an instance of GoogleNetModel model */
    def build(Height:Int,Width:Int)={
      
      /////////////////////////////////////////////////////////////////////////////////////
      //Building the inception module
      def inc(input_size:Int,config:Array[Array[Int]])={
      val depthCat=Concat(2)
      
      val conv1=Sequential()
      conv1.add(SpatialConvolution(input_size,config(1)(1),1,1))
      conv1.add(ReLU(true))
      depthCat.add(conv1)
      
      val conv3=Sequential()
      conv3.add(SpatialConvolution(input_size,config(2)(1),1,1))
      conv3.add(ReLU(true))
      conv3.add(SpatialConvolution(config(2)(1),config(2)(2),3,3,1,1,1,1))
      conv3.add(ReLU(true))
      depthCat.add(conv3)
      
      val conv5=Sequential()
      conv5.add(SpatialConvolution(input_size,config(3)(1),1,1))
      conv5.add(ReLU(true))
      conv5.add(SpatialConvolution(config(3)(1),config(3)(2),5,5,1,1,2,2))
      conv5.add(ReLU(true))/////////////////////////////////////////////////////////////////////////////////////
      depthCat.add(conv5)
      
      val pool=Sequential()
      pool.add(SpatialMaxPooling(config(4)(1),config(4)(1),1,1,1,1))
      pool.add(SpatialConvolution(input_size,config(4)(2),1,1))
      pool.add(ReLU(true))
      depthCat.add(pool)
      depthCat
    }
    /////////////////////////////////////////////////////////////////////////////////////
      
      
    /////////////////////////////////////////////////////////////////////////////////////
    //first layer factorize convolution
    def fac()={
      val conv=Sequential()
      conv.add(Contiguous())
      //View the input as three of one plane
      conv.add(View(-1,1,224,224))
      conv.add(SpatialConvolution(1,8,7,7,2,2,3,3))
      
      val depthWiseConv=ParallelTable()
      depthWiseConv.add(conv) //R
      depthWiseConv.add(conv.cloneModule()) //G
      depthWiseConv.add(conv.cloneModule()) //B
      
      val factorised=Sequential()
      factorised.add(depthWiseConv)
      factorised.add(ReLU(true))
      factorised.add(SpatialConvolution(24,64,1,1))
      factorised.add(ReLU(true))
      factorised
    }
    /////////////////////////////////////////////////////////////////////////////////////
    
    
      //Building the blocks
      def main0=Sequential()
      main0.add(SpatialZeroPadding(0, 224-Width, 0, 224-Height))
      main0.add(fac())
      main0.add(SpatialMaxPooling(3,3,2,2))
      main0.add(SpatialConvolution(64,64,1,1))
      main0.add(ReLU(true))
      main0.add(SpatialConvolution(64,192,3,3,1,1,1,1))
      main0.add(ReLU(true))
      main0.add(SpatialMaxPooling(3,3,2,2))

      main0.add(inc(192,Array(Array(64, 0),Array(96, 128),Array(16, 32),Array(3, 32))))
      main0.add(inc(256,Array(Array(128, 0),Array(128, 192),Array(32, 96),Array(3, 64))))
      main0.add(SpatialAveragePooling(3,3,2,2))
      main0.add(inc(480,Array(Array(192, 0),Array(96, 208),Array(16, 48),Array(3, 64))))
      
      val main1=Sequential()
      main1.add(inc(512,Array(Array(160, 0),Array(112, 224),Array(24, 64),Array(3, 64))))
      main1.add(inc(512,Array(Array(128, 0),Array(128, 256),Array(24, 64),Array(3, 64))))
      main1.add(inc(152,Array(Array(112, 0),Array(144, 288),Array(32, 64),Array(3, 64))))
   
      val main2=Sequential()
      
      main2.add(inc(528,Array(Array(256, 0),Array(160, 320),Array(32, 128),Array(3, 128))))
      main2.add(SpatialMaxPooling(3,3,2,2))
      main2.add(inc(832,Array(Array(256, 0),Array(160, 320),Array(32, 128),Array(3, 128))))
      main2.add(inc(832,Array(Array(384, 0),Array(192, 384),Array(48, 128),Array(3, 128))))
 
      //ocsiliary classifier
      val sftMx0=Sequential()
      sftMx0.add(SpatialAveragePooling(5,5,3,3))
      sftMx0.add(SpatialConvolution(512,128,1,1))
      sftMx0.add(ReLU())   
      sftMx0.add(View(128*4*4))
      //something missing
      sftMx0.add(Linear(128*4*4,1024))
      sftMx0.add(ReLU())
      sftMx0.add(Dropout(0.7))
      sftMx0.add(Linear(1024,1000))
      sftMx0.add(ReLU())
      sftMx0.add(LogSoftMax())
  
      val sftMx1=Sequential()
      sftMx1.add(SpatialAveragePooling(5,5,3,3))
      sftMx1.add(SpatialConvolution(512,128,1,1))
      sftMx1.add(ReLU())   
      sftMx1.add(View(128*4*4))
      //something missing
      sftMx1.add(Linear(128*4*4,1024))
      sftMx1.add(ReLU())
      sftMx1.add(Dropout(0.7))
      sftMx1.add(Linear(1024,1000))
      sftMx1.add(ReLU())
      sftMx1.add(LogSoftMax())
  
      val sftMx2=Sequential()
      sftMx2.add(SpatialAveragePooling(7,7,1,1))
      sftMx2.add(View(1024))
      sftMx2.add(Dropout(0.4))
      sftMx2.add(Linear(1024,1000))
      sftMx2.add(ReLU())
      sftMx2.add(LogSoftMax())
      
      //Logo blocks
      val block2 = Sequential()
      block2.add(main2)
      block2.add(sftMx2)
  
      val split1 = Concat(2)
      split1.add(block2)
      split1.add(sftMx1)
  
      val block1 = Sequential()
      block1.add(main1)
      block1.add(split1)
  
      val split0 = Concat(2)
      split0.add(block1)
      split0.add(sftMx0)
  
      val block0 = Sequential()
      block0.add(main0)/////////////////////////////////////////////////////////////////////////////////////
      block0.add(split0)
  
      val model = block0
      model
  }
}