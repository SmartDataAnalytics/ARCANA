package tech.sda.arcana.spark.neuralnetwork.model
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl.nn

/*
 * Sequential[1464e262]{
  [input -> (1) -> (2) -> (3) -> (4) -> (5) -> (6) -> (7) -> (8) -> (9) -> (10) -> (11) -> (12) -> (13) -> output]
  (1): SpatialConvolution[conv1_5x5](1 -> 6, 5 x 5, 1, 1, 0, 0)
  (2): Tanh[aa8c6e5d]
  (3): SpatialMaxPooling[15dbf799](2, 2, 2, 2, 0, 0)
  (4): SpatialConvolution[conv2_5x5](6 -> 16, 5 x 5, 1, 1, 0, 0)
  (5): Tanh[1d148dac]
  (6): SpatialMaxPooling[e9aeb379](2, 2, 2, 2, 0, 0)
  (7): Reshape[d82a65cb](400)
  (8): Linear[linear_120](400 -> 120)
  (9): Tanh[a6b3fb4f]
  (10): Linear[linear_84](120 -> 84)
  (11): Tanh[59ebc3b5]
  (12): Linear[linear_classnum](84 -> 5)
  (13): LogSoftMax[4dd837ca]
}
 */

/** Object represents LeNetModel takes (32x32) input size */
object LeNet5Model {

  /** creates an instance of LeNetModel model */
  def build(classNum: Int)={
  val LeNet5Model= new Sequential()
  // 1 input channel, 6 output channels, 5x5 convolution kernel
  LeNet5Model.add(SpatialConvolution(nInputPlane=1,nOutputPlane= 6,kernelW= 5,kernelH= 5).setName("conv1_5x5"))//DenseTensor of size 1x6x1x5x5
  //non-linearity (+Z) =max(0,x)
  LeNet5Model.add(Tanh())
  //A max-pooling operation that looks at (1/2 whole whole tensor)x(1/2 whole whole tensor) windows and finds the max.
  //kW=kernel width,kH=kernel height,dW=step size in width,dH=step size in height
  LeNet5Model.add(SpatialMaxPooling(kW=2,kH=2,dW=2,dH=2))
  // 6 input channel, 16 output channels, 5x5 convolution kernel
  LeNet5Model.add(SpatialConvolution(6,16,5,5).setName("conv2_5x5"))//DenseTensor of size 1x16x6x5x5
  //non-linearity (+Z) =max(0,x)
  LeNet5Model.add(Tanh())
  LeNet5Model.add(SpatialMaxPooling(2,2,2,2))
  //reshapes from a 3D tensor of 16x5x5 into 1D tensor of 16*5*5
  LeNet5Model.add(Reshape(Array(16*5*5)))
  //fully connected layer (matrix multiplication between input and weights)
  LeNet5Model.add(Linear(16*5*5,120).setName("linear_120"))
  //non-linearity (+Z) =max(0,x)
  LeNet5Model.add(Tanh())
  LeNet5Model.add(Linear(120,84).setName("linear_84"))
  //non-linearity (+Z) =max(0,x)
  LeNet5Model.add(Tanh())
  LeNet5Model.add(Linear(84,classNum).setName("linear_classnum"))
  LeNet5Model.add(LogSoftMax())
  LeNet5Model
  }
  
  def graph(classNum: Int)={
  val input = Reshape(Array(1, 32, 32)).inputs()
  val conv1 = SpatialConvolution(1, 6, 5, 5).setName("conv1_5x5").inputs(input)
  val tanh1 = Tanh().inputs(conv1)
  val pool1 = SpatialMaxPooling(2, 2, 2, 2).inputs(tanh1)
  val conv2 = SpatialConvolution(6,16,5,5).setName("conv2_5x5").inputs(pool1)
  val tanh2 = Tanh().inputs(conv2)
  val pool2 = SpatialMaxPooling(2, 2, 2, 2).inputs(tanh2)
  val reshape = Reshape(Array(16*5*5)).inputs(pool2)
  val linear_120 = Linear(16*5*5,120).setName("linear_120").inputs(reshape)
  val tanh3 = Tanh().inputs(linear_120)
  val linear_84 = Linear(120,84).setName("linear_84").inputs(tanh3)
  val tanh4 = Tanh().inputs(linear_84)
  val linear_classnum = Linear(84,classNum).setName("linear_classnum").inputs(tanh4)
  val output = LogSoftMax().inputs(linear_classnum)
  Graph(input, output)
  }
  
  
}