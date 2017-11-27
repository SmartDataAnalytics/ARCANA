package tech.sda.arcana.spark.neuralnetwork.model
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._

/** Object represents LeNetModel takes ([even#]>12 X [even#]>12) input size */
object DyLeNet5Model {
  
  def build(Height:Int,Width:Int,classNum: Int)={
    //constraint to make sure that the input is valid 
    if((Height<=12 || Width<=12)||(Height%2!=0 || Width%2!=0)){
      val message:String="error"
      println(message)
      message
    }
    val view:Int=16*((((Height-4)/2)-4)/2)*((((Width-4)/2)-4)/2)
    val LeNet5Model= new Sequential()
    // 1 input channel, 6 output channels, 5x5 convolution kernel
    LeNet5Model.add(SpatialConvolution(nInputPlane=1,nOutputPlane=6,kernelW=5,kernelH=5).setName("conv1_5x5"))//DenseTensor of size 1x6x1x5x5
    //= (exp(x)-exp(-x))/(exp(x)+exp(-x))
    LeNet5Model.add(Tanh())
    //A max-pooling operation that looks at 2x2 windows and finds the max.
    //kW=kernel width,kH=kernel height,dW=step size in width,dH=step size in height
    LeNet5Model.add(SpatialMaxPooling(kW=2,kH=2,dW=2,dH=2))
    // 6 input channel, 16 output channels, 5x5 convolution kernel
    LeNet5Model.add(SpatialConvolution(6,16,5,5).setName("conv2_5x5"))//DenseTensor of size 1x16x6x5x5
    //= (exp(x)-exp(-x))/(exp(x)+exp(-x))
    LeNet5Model.add(Tanh())
    LeNet5Model.add(SpatialMaxPooling(2,2,2,2))
    //reshapes from a 3D tensor of 16x5x5 into 1D tensor of 16*5*5
    LeNet5Model.add(Reshape(Array(view)))
    //fully connected layer (matrix multiplication between input and weights)
    LeNet5Model.add(Linear(view,120).setName("linear_120"))
    //= (exp(x)-exp(-x))/(exp(x)+exp(-x))
    LeNet5Model.add(Tanh())
    LeNet5Model.add(Linear(120,84).setName("linear_84"))
    //= (exp(x)-exp(-x))/(exp(x)+exp(-x))
    LeNet5Model.add(Tanh())
    LeNet5Model.add(Linear(84,10).setName("linear_10"))
    LeNet5Model.add(Tanh())
    //To get one result in the end
    LeNet5Model.add(Linear(10,classNum).setName("linear_classnum"))
    //= (exp(x)-exp(-x))/(exp(x)+exp(-x))
    //LeNet5Model.add(Tanh())
    LeNet5Model.add(LogSoftMax())
    //Printing the model specifications
    //println(LeNet5Model.getParametersTable())
    LeNet5Model
  }
  
  def graph(Height:Int,Width:Int,classNum: Int)={
    val view:Int=16*((((Height-4)/2)-4)/2)*((((Width-4)/2)-4)/2)
    val input = Reshape(Array(1, Height, Width)).inputs()
    val conv1 = SpatialConvolution(1, 6, 5, 5).setName("conv1_5x5").inputs(input)
    val tanh1 = Tanh().inputs(conv1)
    val pool1 = SpatialMaxPooling(2, 2, 2, 2).inputs(tanh1)
    val conv2 = SpatialConvolution(6,16,5,5).setName("conv2_5x5").inputs(pool1)
    val tanh2 = Tanh().inputs(conv2)
    val pool2 = SpatialMaxPooling(2, 2, 2, 2).inputs(tanh2)
    val reshape = Reshape(Array(view)).inputs(pool2)
    val linear_120 = Linear(16*5*5,120).setName("linear_120").inputs(reshape)
    val tanh3 = Tanh().inputs(linear_120)
    val linear_84 = Linear(120,84).setName("linear_84").inputs(tanh3)
    val tanh4 = Tanh().inputs(linear_84)
    val linear_10 = Linear(84,10).setName("linear_10").inputs(tanh4)
    val tanh5 = Tanh().inputs(linear_10)
    val linear_classnum = Linear(10,classNum).setName("linear_classnum").inputs(tanh5)
    val output = LogSoftMax().inputs(linear_classnum)
    Graph(input, output)
  }
  
}