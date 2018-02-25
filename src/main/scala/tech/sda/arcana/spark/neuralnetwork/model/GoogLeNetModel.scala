package tech.sda.arcana.spark.neuralnetwork.model
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.utils.{T, Table}
import com.intel.analytics.bigdl.Module
import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.{Graph, _}
import com.intel.analytics.bigdl._


/** Object represents GoogleNetModel */
object GoogLeNetModel {
  
  
  def build_M(Height:Int,Width:Int,classNum: Int)={
      
    
      def inc_M(input_size:Int,config:Table)={
       val depthCat=Concat(2)
       
       val conv1=Sequential()
       conv1.add(SpatialConvolution(input_size,config[Table](1)(1),1,1))
       conv1.add(ReLU(true))
       depthCat.add(conv1)
       
       val conv3=Sequential()
       conv3.add(SpatialConvolution(input_size,config[Table](2)(1),1,1))
       conv3.add(ReLU(true))
       conv3.add(SpatialConvolution(config[Table](2)(1),config[Table](2)(2),3,3,1,1,1,1))
       conv3.add(ReLU(true))
       depthCat.add(conv3)
       
       val conv5=Sequential()
       conv5.add(SpatialConvolution(input_size,config[Table](3)(1),1,1))
       conv5.add(ReLU(true))
       conv5.add(SpatialConvolution(config[Table](3)(1),config[Table](3)(2),5,5,1,1,2,2))
       conv5.add(ReLU(true))
       depthCat.add(conv5)
      
       val pool=Sequential()
       pool.add(SpatialMaxPooling(config[Table](4)(1),config[Table](4)(1),1,1,1,1))
       pool.add(SpatialConvolution(input_size,config[Table](4)(2),1,1))
       pool.add(ReLU(true))
       depthCat.add(pool)
       depthCat
  }
  
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
    
      val main0 = Sequential()
      main0.add(fac()) 
      main0.add(SpatialConvolution(3, 64, 7, 7, 2, 2, 3, 3))
      main0.add(SpatialMaxPooling(3, 3, 2, 2).ceil())
      main0.add(SpatialConvolution(64, 64, 1, 1)).add(ReLU(true)) 
      main0.add(SpatialConvolution(64, 192, 3, 3, 1, 1, 1, 1)).add(ReLU(true)) 
      main0.add(SpatialMaxPooling(3,3,2,2).ceil())
      main0.add(inc_M(192, T(T( 64), T( 96,128), T(16, 32), T(3, 32)))) 
      main0.add(inc_M(256, T(T(128), T(128,192), T(32, 96), T(3, 64)))) 
      main0.add(SpatialMaxPooling(3, 3, 2, 2).ceil())
      main0.add(inc_M(480, T(T(192), T( 96,208), T(16, 48), T(3, 64)))) 
      
      val main1 = Sequential()
      main1.add(inc_M(512, T(T(160), T(112,224), T(24, 64), T(3, 64)))) 
      main1.add(inc_M(512, T(T(128), T(128,256), T(24, 64), T(3, 64)))) 
      main1.add(inc_M(512, T(T(112), T(144,288), T(32, 64), T(3, 64)))) 
      
      val main2 = Sequential()
      main2.add(inc_M(528, T(T(256), T(160,320), T(32,128), T(3,128)))) 
      main2.add(SpatialMaxPooling(3, 3, 2, 2).ceil())
      main2.add(inc_M(832,T(T(256),T(160,320),T(32,128),T(3,128)))) 
      main2.add(inc_M(832, T(T(384), T(192,384), T(48,128), T(3,128)))) 
      
      val sftMx0 = Sequential() 
      sftMx0.add(SpatialAveragePooling(5, 5, 3, 3))
      sftMx0.add(SpatialConvolution(512, 128, 1, 1)).add(ReLU(true))
      sftMx0.add(View(128*4*4).setNumInputDims(3))
      sftMx0.add(Linear(128*4*4, 1024)).add(ReLU())
      sftMx0.add(Dropout(0.7))
      sftMx0.add(Linear(1024, classNum)).add(ReLU())
      sftMx0.add(LogSoftMax())
      
      val sftMx1 = Sequential() 
      sftMx1.add(SpatialAveragePooling(5, 5, 3, 3))
      sftMx1.add(SpatialConvolution(528, 128, 1, 1)).add(ReLU(true))
      sftMx1.add(View(128*4*4).setNumInputDims(3))
      sftMx1.add(Linear(128*4*4, 1024)).add(ReLU())
      sftMx1.add(Dropout(0.7))
      sftMx1.add(Linear(1024, classNum)).add(ReLU())
      sftMx1.add(LogSoftMax())
      
      val sftMx2 = Sequential() 
      sftMx2.add(SpatialAveragePooling(7, 7, 1, 1))
      sftMx2.add(View(1024).setNumInputDims(3))
      sftMx2.add(Dropout(0.4))
      sftMx2.add(Linear(1024, classNum)).add(ReLU()) 
      sftMx2.add(LogSoftMax())
       
       
       
       //-- Macro blocks 
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
       block0.add(main0)
       block0.add(split0)
       
       val model = block0
       model
  }
  
  

        def inc(inputSize:Int, config:Table, namePrefix:String = ""):Module[Float] = {
            val concat = Concat(2)
            val conv1 = Sequential()
            conv1.add(SpatialConvolution(inputSize,
              config[Table](1)(1), 1, 1, 1, 1)
              .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "1x1"))
            conv1.add(ReLU(true).setName(namePrefix + "relu_1x1"))
            concat.add(conv1)
            val conv3 = Sequential()
            conv3.add(SpatialConvolution(inputSize,
              config[Table](2)(1), 1, 1, 1, 1)
              .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "3x3_reduce"))
            conv3.add(ReLU(true).setName(namePrefix + "relu_3x3_reduce"))
            conv3.add(SpatialConvolution(config[Table](2)(1),
              config[Table](2)(2), 3, 3, 1, 1, 1, 1)
              .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "3x3"))
            conv3.add(ReLU(true).setName(namePrefix + "relu_3x3"))
            concat.add(conv3)
            val conv5 = Sequential()
            conv5.add(SpatialConvolution(inputSize,
              config[Table](3)(1), 1, 1, 1, 1)
              .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "5x5_reduce"))
            conv5.add(ReLU(true).setName(namePrefix + "relu_5x5_reduce"))
            conv5.add(SpatialConvolution(config[Table](3)(1),
              config[Table](3)(2), 5, 5, 1, 1, 2, 2)
              .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "5x5"))
            conv5.add(ReLU(true).setName(namePrefix + "relu_5x5"))
            concat.add(conv5)
            val pool = Sequential()
            pool.add(SpatialMaxPooling(3, 3, 1, 1, 1, 1).ceil().setName(namePrefix + "pool"))
            pool.add(SpatialConvolution(inputSize,
              config[Table](4)(1), 1, 1, 1, 1)
              .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "pool_proj"))
            pool.add(ReLU(true).setName(namePrefix + "relu_pool_proj"))
            concat.add(pool).setName(namePrefix + "output")
        concat
    }
  
  
  /** creates an instance of GoogleNetModel model */
    def build(Height:Int,Width:Int,classNum:Int)={
     
    
      //Building the blocks
          val hasDropout=false        
            val feature1 = Sequential()
            feature1.add(Padding(1,2,3,value=0.0,nIndex=1))
            feature1.add(SpatialZeroPadding(0, 224-Width, 0, 224-Height))
            feature1.add(SpatialConvolution(3, 64, 7, 7, 2, 2, 3, 3, 1, true)
              .setInitMethod(weightInitMethod = Xavier, Zeros)
              .setName("conv1/7x7_s2"))
            feature1.add(ReLU(true).setName("conv1/relu_7x7"))
            feature1.add(SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool1/3x3_s2"))
            feature1.add(SpatialCrossMapLRN(5, 0.0001, 0.75).setName("pool1/norm1"))
            feature1.add(SpatialConvolution(64, 64, 1, 1, 1, 1)
              .setInitMethod(weightInitMethod = Xavier, Zeros)
              .setName("conv2/3x3_reduce"))
            feature1.add(ReLU(true).setName("conv2/relu_3x3_reduce"))
            feature1.add(SpatialConvolution(64, 192, 3, 3, 1, 1, 1, 1)
              .setInitMethod(weightInitMethod = Xavier, Zeros)
              .setName("conv2/3x3"))
            feature1.add(ReLU(true).setName("conv2/relu_3x3"))
            feature1.add(SpatialCrossMapLRN(5, 0.0001, 0.75). setName("conv2/norm2"))
            feature1.add(SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool2/3x3_s2"))
            feature1.add(inc(192, T(T(64), T(96, 128), T(16, 32), T(32)), "inception_3a/"))
            feature1.add(inc(256, T(T(128), T(128, 192), T(32, 96), T(64)), "inception_3b/"))
            feature1.add(SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool3/3x3_s2"))
            feature1.add(inc(480, T(T(192), T(96, 208), T(16, 48), T(64)), "inception_4a/"))
        
            val output1 = Sequential()
            output1.add(SpatialAveragePooling(5, 5, 3, 3).ceil().setName("loss1/ave_pool"))
            output1.add(SpatialConvolution(512, 128, 1, 1, 1, 1).setName("loss1/conv"))
            output1.add(ReLU(true).setName("loss1/relu_conv"))
            output1.add(View(128 * 4 * 4).setNumInputDims(3))
            output1.add(Linear(128 * 4 * 4, 1024).setName("loss1/fc"))
            output1.add(ReLU(true).setName("loss1/relu_fc"))
            if (hasDropout) output1.add(Dropout(0.7).setName("loss1/drop_fc"))
            output1.add(Linear(1024, classNum).setName("loss1/classifier"))
            output1.add(LogSoftMax().setName("loss1/loss"))
        
            val feature2 = Sequential()
            feature2.add(inc(512, T(T(160), T(112, 224), T(24, 64), T(64)), "inception_4b/"))
            feature2.add(inc(512, T(T(128), T(128, 256), T(24, 64), T(64)), "inception_4c/"))
            feature2.add(inc(512, T(T(112), T(144, 288), T(32, 64), T(64)), "inception_4d/"))
        
            val output2 = Sequential()
            output2.add(SpatialAveragePooling(5, 5, 3, 3).setName("loss2/ave_pool"))
            output2.add(SpatialConvolution(528, 128, 1, 1, 1, 1).setName("loss2/conv"))
            output2.add(ReLU(true).setName("loss2/relu_conv"))
            output2.add(View(128 * 4 * 4).setNumInputDims(3))
            output2.add(Linear(128 * 4 * 4, 1024).setName("loss2/fc"))
            output2.add(ReLU(true).setName("loss2/relu_fc"))
            if (hasDropout) output2.add(Dropout(0.7).setName("loss2/drop_fc"))
            output2.add(Linear(1024, classNum).setName("loss2/classifier"))
            output2.add(LogSoftMax().setName("loss2/loss"))
        
            val output3 = Sequential()
            output3.add(inc(528, T(T(256), T(160, 320), T(32, 128), T(128)),
              "inception_4e/"))
            output3.add(SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool4/3x3_s2"))
            output3.add(inc(832, T(T(256), T(160, 320), T(32, 128), T(128)),
              "inception_5a/"))
            output3.add(inc(832, T(T(384), T(192, 384), T(48, 128), T(128)),
              "inception_5b/"))
            output3.add(SpatialAveragePooling(7, 7, 1, 1).setName("pool5/7x7_s1"))
            if (hasDropout) output3.add(Dropout(0.4).setName("pool5/drop_7x7_s1"))
            output3.add(View(1024).setNumInputDims(3))
            output3.add(Linear(1024, classNum)
              .setInitMethod(weightInitMethod = Xavier, Zeros).setName("loss3/classifier"))
            output3.add(LogSoftMax().setName("loss3/loss3"))
        
            val split2 = Concat(2).setName("split2")
            split2.add(output3)
            split2.add(output2)
        
            val mainBranch = Sequential()
            mainBranch.add(feature2)
            mainBranch.add(split2)
        
            val split1 = Concat(2).setName("split1")
            split1.add(mainBranch)
            split1.add(output1)
        
            val model = Sequential()
        
            model.add(feature1)
            model.add(split1)
        
        model
      }
    
   
    def graph(Height:Int,Width:Int,classNum: Int)={

    
       def inc_g(input: ModuleNode[Float], inputSize: Int, config: Table, namePrefix : String):ModuleNode[Float] = {
   
        val conv1x1 = SpatialConvolution(inputSize, config[Table](1)(1), 1, 1, 1, 1)
            .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "1x1").inputs(input)
        val relu1x1 = ReLU(true).setName(namePrefix + "relu_1x1").inputs(conv1x1)
    
        val conv3x3_1 = SpatialConvolution(inputSize, config[Table](2)(1), 1, 1, 1, 1).setInitMethod(
          weightInitMethod = Xavier, Zeros).setName(namePrefix + "3x3_reduce").inputs(input)
        val relu3x3_1 = ReLU(true).setName(namePrefix + "relu_3x3_reduce").inputs(conv3x3_1)
        val conv3x3_2 = SpatialConvolution(
          config[Table](2)(1), config[Table](2)(2), 3, 3, 1, 1, 1, 1)
          .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "3x3").inputs(relu3x3_1)
        val relu3x3_2 = ReLU(true).setName(namePrefix + "relu_3x3").inputs(conv3x3_2)
    
        val conv5x5_1 = SpatialConvolution(inputSize, config[Table](3)(1), 1, 1, 1, 1).setInitMethod(
          weightInitMethod = Xavier, Zeros).setName(namePrefix + "5x5_reduce").inputs(input)
        val relu5x5_1 = ReLU(true).setName(namePrefix + "relu_5x5_reduce").inputs(conv5x5_1)
        val conv5x5_2 = SpatialConvolution(
          config[Table](3)(1), config[Table](3)(2), 5, 5, 1, 1, 2, 2)
          .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "5x5").inputs(relu5x5_1)
        val relu5x5_2 = ReLU(true).setName(namePrefix + "relu_5x5").inputs(conv5x5_2)
    
        val pool = SpatialMaxPooling(3, 3, 1, 1, 1, 1).ceil()
          .setName(namePrefix + "pool").inputs(input)
        val convPool = SpatialConvolution(inputSize, config[Table](4)(1), 1, 1, 1, 1).setInitMethod(
          weightInitMethod = Xavier, Zeros).setName(namePrefix + "pool_proj").inputs(pool)
        val reluPool = ReLU(true).setName(namePrefix + "relu_pool_proj").inputs(convPool)
    
        JoinTable(2, 0).inputs(relu1x1, relu3x3_2, relu5x5_2, reluPool)
        }  
      
      
    val input = Input()
    val conv1 = SpatialConvolution(3, 64, 7, 7, 2, 2, 3, 3, 1, false)
      .setInitMethod(weightInitMethod = Xavier, Zeros)
      .setName("conv1/7x7_s2").inputs(input)
    val relu1 = ReLU(true).setName("conv1/relu_7x7").inputs(conv1)
    val pool1 = SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool1/3x3_s2").inputs(relu1)
    val lrn1 = SpatialCrossMapLRN(5, 0.0001, 0.75).setName("pool1/norm1").inputs(pool1)
    val conv2 = SpatialConvolution(64, 64, 1, 1, 1, 1)
      .setInitMethod(weightInitMethod = Xavier, Zeros)
      .setName("conv2/3x3_reduce").inputs(lrn1)
    val relu2 = ReLU(true).setName("conv2/relu_3x3_reduce").inputs(conv2)
    val conv3 = SpatialConvolution(64, 192, 3, 3, 1, 1, 1, 1)
      .setInitMethod(weightInitMethod = Xavier, Zeros)
      .setName("conv2/3x3").inputs(relu2)
    val relu3 = ReLU(true).setName("conv2/relu_3x3").inputs(conv3)
    val lrn2 = SpatialCrossMapLRN(5, 0.0001, 0.75). setName("conv2/norm2").inputs(relu3)
    val pool2 = SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool2/3x3_s2").inputs(lrn2)
    val layer1 = inc_g(pool2, 192, T(T(64), T(96, 128), T(16, 32), T(32)),
      "inception_3a/")
    val layer2 = inc_g(layer1, 256, T(T(128), T(128, 192), T(32, 96), T(64)),
      "inception_3b/")
    val pool3 = SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool3/3x3_s2").inputs(layer2)
    val feature1 = inc_g(pool3, 480, T(T(192), T(96, 208), T(16, 48), T(64)),
      "inception_4a/")

    val pool2_1 = SpatialAveragePooling(5, 5, 3, 3).ceil()
      .setName("loss1/ave_pool").inputs(feature1)
    val loss2_1 = SpatialConvolution(512, 128, 1, 1, 1, 1).setName("loss1/conv").inputs(pool2_1)
    val relu2_1 = ReLU(true).setName("loss1/relu_conv").inputs(loss2_1)
    val view2_1 = View(128 * 4 * 4).setNumInputDims(3).inputs(relu2_1)
    val linear2_1 = Linear(128 * 4 * 4, 1024).setName("loss1/fc").inputs(view2_1)
    val relu2_2 = ReLU(true).setName("loss1/relu_fc").inputs(linear2_1)
    val drop2_1 = Dropout(0.7).setName("loss1/drop_fc").inputs(relu2_2) 
    val classifier2_1 = Linear(1024, classNum).setName("loss1/classifier").inputs(drop2_1)
    val output1 = LogSoftMax().setName("loss1/loss").inputs(classifier2_1)

    val layer3_1 = inc_g(feature1, 512, T(T(160), T(112, 224), T(24, 64), T(64)),
      "inception_4b/")
    val layer3_2 = inc_g(layer3_1, 512, T(T(128), T(128, 256), T(24, 64), T(64)),
      "inception_4c/")
    val feature2 = inc_g(layer3_2, 512, T(T(112), T(144, 288), T(32, 64), T(64)),
      "inception_4d/")

    val pool4_1 = SpatialAveragePooling(5, 5, 3, 3).setName("loss2/ave_pool").inputs(feature2)
    val conv4_1 = SpatialConvolution(528, 128, 1, 1, 1, 1).setName("loss2/conv").inputs(pool4_1)
    val relu4_1 = ReLU(true).setName("loss2/relu_conv").inputs(conv4_1)
    val view4_1 = View(128 * 4 * 4).setNumInputDims(3).inputs(relu4_1)
    val linear4_1 = Linear(128 * 4 * 4, 1024).setName("loss2/fc").inputs(view4_1)
    val relu4_2 = ReLU(true).setName("loss2/relu_fc").inputs(linear4_1)
    val drop4_1 = Dropout(0.7).setName("loss2/drop_fc").inputs(relu4_2)
    val linear4_2 = Linear(1024, classNum).setName("loss2/classifier").inputs(drop4_1)
    val output2 = LogSoftMax().setName("loss2/loss").inputs(linear4_2)

    val layer5_1 = inc_g(feature2, 528, T(T(256), T(160, 320), T(32, 128), T(128)),
      "inception_4e/")
    val pool5_1 = SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool4/3x3_s2").inputs(layer5_1)
    val layer5_2 = inc_g(pool5_1, 832, T(T(256), T(160, 320), T(32, 128), T(128)),
      "inception_5a/")
    val layer5_3 = inc_g(layer5_2, 832, T(T(384), T(192, 384), T(48, 128), T(128)),
      "inception_5b/")
    val pool5_4 = SpatialAveragePooling(7, 7, 1, 1).setName("pool5/7x7_s1").inputs(layer5_3)
    val drop5_1 = Dropout(0.4).setName("pool5/drop_7x7_s1")
      .inputs(pool5_4)
    val view5_1 = View(1024).setNumInputDims(3).inputs(drop5_1)
    val linear5_1 = Linear(1024, classNum)
      .setInitMethod(weightInitMethod = Xavier, Zeros).setName("loss3/classifier").inputs(view5_1)
    val output3 = LogSoftMax().setName("loss3/loss3").inputs(linear5_1)

    val split2 = JoinTable(2, 0).setName("split2").inputs(output3, output2, output1)
    Graph(input, split2)
    }
   
    
    /*
     *	     +-------+      +-------+        +-------+
  					 | main0 +--+---> main1 +----+---> main2 +----+
  					 +-------+ |   +-------+    |   +-------+    |
            	   	     |                |                |
        	    	       | +----------+   | +----------+   | +----------+
        		  	      +-> softMax0 +-+  +-> softMax1 +-+ +-> softMax2 +-+
        		             +----------+ |   +----------+ |   +----------+ |
            		                      |                |                |   +-------+
            		                      +----------------v----------------v--->  out  |
             			                                                          +-------+
     */
}