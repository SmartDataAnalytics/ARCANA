package tech.sda.arcana.spark
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.collection.mutable.ListBuffer
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.nn.MSECriterion
import com.intel.analytics.bigdl.utils.T
import shapeless._0
import tech.sda.arcana.spark.neuralnetwork.model.LeNet5Model
import tech.sda.arcana.spark.classification.cnn._
import tech.sda.arcana.spark.representation._

object flow {
  
 
  def main(args: Array[String]) = {
      //The following are for .Jar
      //Glove path
      //val glovePath:String=args(0)
      //Questions path
      //val questionsPath:String=args(1)
      //Mappings Path
      //val mappingsPath:String=args(2)
      //App name to store the results
      //val appName:String=args(3)
      //For optimization this could calculated separated
      //longestWordsSeq:Int=args(4)
      //Length of the word-vec used
      //vectorLength:Int=args(5)
      val accuracy:Boolean=true
      
      //Path to the vectorial representation 
      val vectorialRepresentationPath="/home/mhd/Desktop/ARCANA Resources/glove.6B/glove.6B.50d.txt"
      //Path to the questions
      val questionPath="/home/mhd/Desktop/Data_Set/TestNow.txt"
      //Path to the questions mappings 
      val mappingsPath="/home/mhd/Desktop/Data_Set/Mapping.txt"
      //Initialize the class responsible of the connection between BigDl and Spark
      val sparkBigDlInitializer=new SparkBigDlInitializer()
      //Initialize the Sparkcontext using the BigDL engine with setting the Application name
      val sc=sparkBigDlInitializer.initialize(model="Test")
      //Initialize the class responsible of dealing with the questions
      val questionInitializer=new QuestionsInitializer(sparkContext=sc) 
      //Read the questions real mappings
      val mappings = sc.textFile(mappingsPath)
      //Parallelize the vectorial representation on the clusters
      val vectorialRepresentation = sc.textFile(vectorialRepresentationPath)
      //Parallelize the questions on the clusters
      val questionsWithoutCleaning=sc.textFile(questionPath)
      //Cleaning the questions and add spaces between punctuations and other chars
      val questions = questionsWithoutCleaning.map(questionInitializer.clean)
      //Add order numbers to the questions 
      val orderedQuestions=questions.zipWithIndex().map{case(line,i) => i.toString+" "+line}
      //Initialize the class responsible of the vectorial representation
      val vectorizationDelegator=new VectorizationDelegator(sparkContext=sc,vectorLength=50)
      //Build the spark structure (RDD) for the vectorial representing using Glov  
      val parsedVectorialRepresentation = vectorialRepresentation.map(vectorizationDelegator.ParseVecGlov)
      //Build the spark structure (RDD) pool of the questions' words with two ids on for the sentence and the other for each word inside each question
      val parsedQuestions=orderedQuestions.flatMap(questionInitializer.parseQuestion)
      //Map the vectorial representation with the words spark pool and produce another structure have all the important info
      val priTensorInfoNoCleaning= parsedQuestions.join(parsedVectorialRepresentation)
      //Discard unnecessary data
      val priTensorInfo= priTensorInfoNoCleaning.map{case(a,b)=>b}
      //Group the info related to one question in one spark structure (RDD)
      val groupedpriTensorInfo=priTensorInfo.groupBy(x=>x._1._1)
      //Initialize the class responsible of mapping questions, vectoral representation RDD to RDD tensors
      //If you want to extract the dimensions of the tensors dynamically uncomment the following line
      //val questionTensorTransformer=new QuestionTensorTransformer(sparkContext=sc,longestWordsSeq=questionInitializer.calculateLongestWordsSeq(questions),vectorLength=50)
      val questionTensorTransformer=new QuestionTensorTransformer(sparkContext=sc,longestWordsSeq=40,vectorLength=50)
      //Transform questions spark structure (RDD) to Tensors (matrices)
      val tensors=groupedpriTensorInfo.map(questionTensorTransformer.transform)
      //Initialize the class responsible for building the training sample
      val sampler=new TensorSampleTransformer(sparkContext=sc)
      //Initialize the questions' mappings
      val maps=mappings.map(sampler.mappingInit)
      //Map each tensor with its label
      val labeledTensors=maps.join(tensors)
      //Build samples
      val samples=labeledTensors.map(sampler.initializeAllSamples)
      //Initialize the class responsible for training the neural network models
      val trainer=new Trainer(lossfun=2,model=3,height=40,width=50,classNum=5,validation=accuracy)
      //Initialize the train samples 
      val trainSamples=samples.filter(x=>x._1==1).map{case(a,b)=>b}
      //Initialize the test samples to calculate the accuracy
      if(accuracy){
      val testSamples=samples.filter(x=>x._1==0).map{case(a,b)=>b}
      //To track the training and testing on the tensorboard use the following line
      //If you don't want to visualise the training process comment the following line
      trainer.visualiseAndValidate(logdir="/home/mhd/Desktop/bigdl_summaries",appName="testAppXXX",testData=testSamples,batchS=3)
      }  
      else{
      //To track the training without testing on the tensorboard use the following line
      trainer.visualise(logdir="/home/mhd/Desktop/bigdl_summaries",appName="testAppXXX",batchS=4)
      }
      //build the Employee responsible for the training
      val employee=trainer.build(samples=trainSamples,batch=4)
      //Train the neural network model
      employee.optimize()
      println("Done")
      }
}
