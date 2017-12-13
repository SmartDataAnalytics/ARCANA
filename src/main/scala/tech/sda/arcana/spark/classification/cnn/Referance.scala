package tech.sda.arcana.spark.classification.cnn
import java.io._
import org.apache.spark.rdd.RDD
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.nn.View
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.collection.mutable.ListBuffer
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.dataset.MiniBatch
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.nn.MSECriterion
import com.intel.analytics.bigdl.utils.T
import shapeless._0
import tech.sda.arcana.spark.neuralnetwork.model.LeNet5Model
import tech.sda.arcana.spark.neuralnetwork.model.DyLeNet5Model
import com.intel.analytics.bigdl.nn.Reshape
import com.intel.analytics.bigdl.nn.Module
import tech.sda.arcana.spark.neuralnetwork.model.AlexNetModel
import tech.sda.arcana.spark.neuralnetwork.model.GoogLeNetModel
import com.intel.analytics.bigdl.nn._

object Referance {
  //---------------------------------------------------------
  //Define the width and height of the tensor
  val width=10
  val height=10
  //Build any tensor manually with filling it values as desired
  val tensor=Tensor[Float](width,height)
  //The storage used to fill the real tensor
  val tensorStorage= tensor.storage()      
  //define the tensor index
  var tI=0        
  //to fill the tensor  
  for(value <- 0 to (width*height)-1){
    tensorStorage(tI)=value
    tI=tI+1
  }
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Different dimensions of tensors
  var tensorF=Tensor[Float](2,2)
  println("--------------2,2--------------------------")
  println(tensorF)//just like 2D array
  println("--------------3,3--------------------------")
  tensorF=Tensor[Float](3,3)
  println(tensorF)//just like 3D array (cube)
  println("--------------3,3,3--------------------------")
  tensorF=Tensor[Float](3,3,3)
  println(tensorF)//three layers and each layer is 3*3 (cube)
  println("--------------2,2,2--------------------------")
  tensorF=Tensor[Float](2,2,2)
  println(tensorF)//two layers and each layers is 2*2 (array)
  println("--------------4,2,3--------------------------")
  tensorF=Tensor[Float](4,2,3)
  println(tensorF)//four layers and each layers is 2*3
  println("--------------4,2,3,5--------------------------")
  tensorF=Tensor[Float](4,2,3,5)
  println(tensorF)////eight layers (4*2) and each layers is 3*5
  //the follwing printed layers
  //(1,1,.,.)...(1,2,.,.)...(2,1,.,.)...(2,2,.,.)...(3,1,.,.) 
  //(3,2,.,.)...(4,1,.,.)...(4,2,.,.)
  println("--------------4,2,3,9--------------------------")
  tensorF=Tensor[Float](4,2,3,9)
  println(tensorF)//eight layers (4*2) and each layers is 3*9
  println("--------------4,4,3,9,4--------------------------")
  tensorF=Tensor[Float](4,4,3,9,4)
  println(tensorF)//48 layers (4*4*3) and each layers is 9*4

  
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Working with Iterators
  val it = Iterator(("Mohamad",(("m",1),(1,2,3))))
  //Convert to any other shape like sequences, arrays and enter their values 
  val itToArray=it.toArray
  //Entering values and sorting depend them      
  val itToSeq=(it.toSeq).sortBy(_._2._1._1)
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //List important operations on neural networks models
  println("------------output-----------------")  
  println(LeNet5Model.build(5).output)//null do not use
  println("------------evaluate-----------------")
  println(LeNet5Model.build(5).evaluate())//draw the deep neural structure
  println("------------checkEngineType-----------------")
  println(LeNet5Model.build(5).checkEngineType())//draw the deep neural structure (same)
  println("------------isTraining-----------------")
  println(LeNet5Model.build(5).isTraining())//true or false
  println("-------------training----------------")
  println(LeNet5Model.build(5).training())//draw the deep neural structure (same)
  println("-----------getTimes------------------")
  println(LeNet5Model.build(5).getTimes())          //var y=x.storage()//produce time tubels
  println("-----------------------------")
  //in addition to predict for RDD samples and forward for Tensors
  //To check the output for a single input tensor
  val dummyTensor=Tensor[Float](1,20,50).rand()
  println(DyLeNet5Model.build(Height=20,Width=50,classNum=5).forward(dummyTensor))
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Understanding the different between View and Reshape layers
  println("------------Tensor[Float](1,5,5).rand()------------")
  val fiTensor=Tensor[Float](1,5,5).rand()
  println(fiTensor)
  println("-----View(25).forward(Tensor[Float](1,5,5).rand())--------")
  println(View(25).forward(fiTensor))
  println("-----View(25).forward(Tensor[Float](1,5,5).rand())--------")
  println(Reshape(Array(25)).forward(fiTensor))
  
  println("------------Tensor[Float](1,5,4).rand()------------")
  val fiTensor2=Tensor[Float](1,5,4).rand          //var y=x.storage()()
  println(fiTensor2)
  println("-----View(25).forward(Tensor[Float](1,5,4).rand())--------")
  println(View(20).forward(fiTensor2))
  println("-----View(25).forward(Tensor[Float](1,5,4).rand())--------")
  println(Reshape(Array(20)).forward(fiTensor2))
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Scenarios for looping
  //RDD.collect().foreach(println)
  //RDD.foreach{x=>printf("\nString= %s Line= %s Word= %s",x._1,x._2._1._1,x._2._1._2) }
  //RDD.foreach{println("---------------StART---------------------")
  //               x=>println(x)
  //               println("----------------END----------------------")}
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Check the built tensors values
  /*
    for(tensor <- Array of tensors){
    var storage=tensor.storage()
      for(i <- 0 to 999){
        print((storage(i)).toString()+", ")
      }
    println("---------------------")
  }
   Do not treat the tensors like arrays because they are not!
   println(tensor(index)) will print all the lements in the tensor
   and tensor(index)(index) will produce a huge error
  */
  //---------------------------------------------------------
  
  
  //---------------------------------------------------------
  //BigDl previous manual environment setting
  //-Dspark.master=local[1] 
  //-Dspark.executor.cores=1 
  //-Dspark.total.executor.cores=1 
  //-Dspark.executor.memory=1g 
  //-Dspark.driver.memory=1g 
  //-Xmx1024m -Xms1024m
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Build a sample
  println("-----------------Begin-----------------------")
  val image=Tensor[Float](3,5,5).rand
  val label=Tensor(T(1f))    
  val sample=Sample(image, label)
  println(image)
  println("------------------#1#----------------------")
  println(label)
  println("------------------#2#----------------------")
  println(sample)
  println("------------------#3#----------------------")
  //convert to set
  val test=Tensor[Float](1,3,3).set()
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //dealing with buffers
  val senRep:ListBuffer[Array[String]]=ListBuffer()
  senRep+=Array("1", "28", "28")
  senRep+=Array("1", "28", "28")
  senRep+=Array("1", "28", "28")
  val xx=Tensor[Float](3,3)
  val ss= xx.storage()
  //check if it can be done without new variable
  val revsenRep=senRep.reverse
  var temp:Array[String] = new Array[String](3)
  
  var h=0
  for(i <- 0 to revsenRep.size-1){
    temp=revsenRep(i)
    for(j <- 0 to temp.size-1) {
      ss(h) =temp(j).toFloat
      h=h+1
    }
  }
  println(xx)
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Use spark to match,map or join on actual gathered data 
  /* Data:
   * val sentense=Array("my","name","is","ghost")
     Create a SparkContext using every core of the local machine
     val sc = new SparkContext("local[*]", "MinTemperatures")      
     Read each line of input data
     val lines = sc.textFile("/home/mhd/Desktop/ARCANA Resources/glove.6B/glove.6B.50d.txt")
     val parsedLines = lines.map(parseLine)
     using filter 
     val representation = parsedLines.filter( (x) => (x._1 == "the") )
     val representation = parsedLines.filter( (x) => (sentense.contains(x._1)) )
     gather the answers and continue arranging without Spark   
     val result = representation.collect()  
        //this section has been done locally because the data is rather small
        val senRep:ListBuffer[Array[String]]=ListBuffer()
        for(i <- 0 to sentenceWordCount-1)
          for(j<-result)
            if(j._1 == sentense(i))
              senRep += j._2
        
        //achtung this is done locally 
        for(i<-senRep){
          for(j<-i){
            print(j)
            print(" ")
          }
          println()
        }
   */
  
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Using table for the inception module configuration
  //val conf:Tabel={{1},{2},{4}}
  val config:Array[Array[Int]]=Array(Array(64, 0),Array(96, 128),Array(16, 32),Array(3, 32))
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Save the topologies of the neural modesl and the structure for each one
  println("-------------------DyLeNet5Model------------------------")
  val first=DyLeNet5Model
  println(first.build(20,20,5).evaluate())
  first.graph(20,20,5).saveGraphTopology("/home/mhd/Desktop/bigdl_summaries/first")
  println("done DyLeNet5Model")
  println("-------------------LeNet5Model------------------------")
  val second=LeNet5Model
  println(second.build(5).evaluate())
  second.graph(5).saveGraphTopology("/home/mhd/Desktop/bigdl_summaries/second")
  println("done LeNet5Model")
  println("-------------------AlexNetModel-------------------------")
  val fourth=AlexNetModel
  println(fourth.build(10,10,5).evaluate())
  fourth.graph(10,10,5).saveGraphTopology("/home/mhd/Desktop/bigdl_summaries/fourth")
  println("done AlexNetModel")
  println("--------------------GoogLeNetModel------------------------")
  val fifth=GoogLeNetModel
  println(fifth.build(10,10,5).evaluate())
  fifth.graph(10,10,5).saveGraphTopology("/home/mhd/Desktop/bigdl_summaries/fifth")
  println("done GoogLeNetModel")
  println("--------------------------------------------")
  //---------------------------------------------------------
  
  //---------------------------------------------------------
  //Save the topologies of the neural models and the structure for each one
   println("-------------------DyLeNet5Model------------------------")
   DyLeNet5Model.build(20,20,5).forward(Tensor[Float](1,20,20).rand())
   println("-------------------LeNet5Model------------------------")
   LeNet5Model.build(5).forward(Tensor[Float](1,32,32).rand())
   println("-------------------AlexNetModel-------------------------")
   AlexNetModel.build(100,100,5).forward(Tensor[Float](1,100,100).rand())
   println("--------------------GoogLeNetModel------------------------")
   GoogLeNetModel.build(100,100,5).forward(Tensor[Float](1,100,100).rand())
  //---------------------------------------------------------
   
   //---------------------------------------------------------
   //contiguity (it did nothing !)
    val testCon = Tensor(5).range(1, 5, 1)
    println("-----Contiguous().forward--------")
    println(testCon)
    println(Contiguous().forward(testCon) )
    //---------------------------------------------------------
    
    //---------------------------------------------------------
    //Paradox why logsoftmax works only after using View (put it in one dim)
    val dd=Tensor[Float](T(T(1.0f),T(2.0f),T(3.0f),T(4.0f),T(1.0f),T(2.0f),T(3.0f)))
    println(dd)
    println(LogSoftMax().forward(View().forward(dd)))
    println("-----------------------------------")
    println(LogSoftMax().forward(Tensor[Float](T(T(1.0f),T(2.0f),T(3.0f),T(4.0f),T(1.0f),T(2.0f),T(3.0f)))))
    println("***")
    println(Tensor[Float](T(T(0f),T(1f))))
    println(View().forward(Tensor[Float](T(T(0f),T(1f)))))
    println("***")
    //---------------------------------------------------------
}