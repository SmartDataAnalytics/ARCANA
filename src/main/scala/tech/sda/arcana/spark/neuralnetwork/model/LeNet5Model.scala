package tech.sda.arcana.spark.neuralnetwork.model
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl.nn

/** Object represents LeNetModel takes (32x32) input size */
object LeNet5Model {

  /** creates an instance of LeNetModel model */
  def build()={
  val LeNet5Model= new Sequential()
  // 1 input channel, 6 output channels, 5x5 convolution kernel
  LeNet5Model.add(SpatialConvolution(1, 6, 5, 5))//DenseTensor of size 1x6x1x5x5
  //non-linearity (+Z)
  LeNet5Model.add(ReLU())
  //A max-pooling operation that looks at (1/2 whole whole tensor)x(1/2 whole whole tensor) windows and finds the max.
  LeNet5Model.add(SpatialMaxPooling(2,2,2,2))
  // 6 input channel, 16 output channels, 5x5 convolution kernel
  LeNet5Model.add(SpatialConvolution(6,16,5,5))//DenseTensor of size 1x16x6x5x5
  //non-linearity (+Z)
  LeNet5Model.add(ReLU())
  LeNet5Model.add(SpatialMaxPooling(2,2,2,2))
  //reshapes from a 3D tensor of 16x5x5 into 1D tensor of 16*5*5
  LeNet5Model.add(View(16*5*5))
  //fully connected layer (matrix multiplication between input and weights)
  LeNet5Model.add(Linear(16*5*5,120))
  LeNet5Model.add(ReLU())
  LeNet5Model.add(Linear(120,84))
  LeNet5Model.add(ReLU())
  LeNet5Model.add(Linear(84,10))
  LeNet5Model.add(LogSoftMax())
  //Printing the model specifications
  println(LeNet5Model.getParametersTable())
  LeNet5Model
  }
}