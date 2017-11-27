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
import tech.sda.arcana.spark.classification.cnn.Core
import tech.sda.arcana.spark.neuralnetwork.model.LeNet5Model
import tech.sda.arcana.spark.classification.cnn._
import tech.sda.arcana.spark.representation._

object flow {
  
 
  def main(args: Array[String]) = {
      //Path to the vectorial representation 
      val vectorialRepresentationPath="/home/mhd/Desktop/ARCANA Resources/glove.6B/glove.6B.50d.txt"
      //Path to the questions
      val questionPath="/home/mhd/Desktop/Data Set/TestNow.txt"
      //Initialize the class responsible of the connection between BigDl and Spark
      val sparkBigDlInitializer=new SparkBigDlInitializer()
      //Initialize the Sparkcontext using the BigDL engine with setting the Application name
      val sc=sparkBigDlInitializer.initialize(model="Test")
      //Initialize the class responsible of dealing with the questions
      val questionInitializer=new QuestionsInitializer(sparkContext=sc) 
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
      val questionTensorTransformer=new QuestionTensorTransformer(sparkContext=sc,longestWordsSeq=20,vectorLength=50)
      //Transform questions spark structure (RDD) to Tensors (matrices)
      val Tensors=groupedpriTensorInfo.map(questionTensorTransformer.transform)
      //Initialize the class responsible for building the training sample
      val sampler=new TensorSampleTransformer(sparkContext=sc)
      //Build positive samples
      val samples=Tensors.map(sampler.initializePositiveSample)
      //Initialize the class responsible for training the neural network models
      val trainer=new Trainer(lossfun=2,model=3,height=20,width=50,classNum=5)
      //To track the training on the tensorboard use the following line
      //If you don't want to visualise the training process comment the following line
      //trainer.visualise(logdir="/home/mhd/Desktop/bigdl_summaries",appName="testAppXXX",testData=samples,batchS=3)
      //build the Employee responsible for the training
      val employee=trainer.build(samples=samples,batch=3)
      //Train the neural network model
      employee.optimize()
      println("Done")
      }
}