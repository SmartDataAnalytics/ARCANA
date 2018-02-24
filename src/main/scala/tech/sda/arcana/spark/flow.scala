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
      //Initialize the class responsible of the connection between BigDl and Spark
      val sparkBigDlInitializer=new SparkBigDlInitializer()
      //Initialize the Sparkcontext using the BigDL engine with setting the Application name
      val sc=sparkBigDlInitializer.initialize(model="Test")
      //Initialize the class responsible of dealing with the questions
      val questionInitializer=new QuestionsInitializer(sparkContext=sc) 
      //Read the questions real mappings
      val mappings = sc.textFile(args(2))
      //Parallelize the vectorial representation on the clusters
      val vectorialRepresentation = sc.textFile(args(0))
      //Parallelize the questions on the clusters
      val questionsWithoutCleaning=sc.textFile(args(1))
      //Cleaning the questions and add spaces between punctuations and other chars
      val questions = questionsWithoutCleaning.map(questionInitializer.clean)
      //Add order numbers to the questions 
      val orderedQuestions=questions.zipWithIndex().map{case(line,i) => i.toString+" "+line}
      //Initialize the class responsible of the vectorial representation
      val vectorizationDelegator=new VectorizationDelegator(sparkContext=sc,vectorLength=args(5).toInt)
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
      //val questionTensorTransformer=new QuestionTensorTransformer(sparkContext=sc,longestWordsSeq=questionInitializer.calculateLongestWordsSeq(questions),vectorLength=args(5).toInt)
      val questionTensorTransformer=new QuestionTensorTransformer(sparkContext=sc,longestWordsSeq=args(4).toInt ,vectorLength=args(5).toInt)
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
      //Initialize the class responsible for training the neural network models                                                           0.001f
      val trainer=new Trainer(lossfun=2,model=args(11).toInt,height=args(4).toInt ,width=args(5).toInt,classNum=args(12).toInt,validation=args(6).toBoolean ,learningrate=args(7).toFloat)
      //Initialize the train samples 
      val trainSamples=samples.filter(x=>x._1==1).map{case(a,b)=>b}
      //Initialize the test samples to calculate the accuracy
      val testSamples=samples.filter(x=>x._1==0).map{case(a,b)=>b}
      if(args(6).toBoolean){
      //To track the training and testing on the tensorboard use the following line
      //If you don't want to visualise the training process comment the following line                                               0.3f
      trainer.visualiseAndValidate(logdir=args(13),appName=args(3),testData=testSamples,batchS=args(14).toInt ,minloss=args(9).toFloat ,maxEpochs=args(8).toInt)
      }  
      else{
      //To track the training without testing on the tensorboard use the following line
      trainer.visualise(logdir=args(13),appName=args(3),maxEpochs=args(8).toInt)
      }
      //build the Employee responsible for the training
      val employee=trainer.build(samples=trainSamples,batch=args(10).toInt)
      //Train the neural nvaletwork model
      val trained_model=employee.optimize()
        if(args(6).toBoolean){
          (trained_model.evaluate(testSamples, Array(new Top1Accuracy), None)).foreach(println)
        }
      }
}
