package tech.sda.arcana.spark
import org.apache.spark.rdd.RDD
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
import tech.sda.arcana.spark.classification.cnn.Core
import tech.sda.arcana.spark.neuralnetwork.model.LeNet5Model
import tech.sda.arcana.spark.classification.cnn._
import tech.sda.arcana.spark.representation._

object flow {
  
  def main(args: Array[String]) = {
      val sparkBigDlInitializer=new SparkBigDlInitializer()
      val sc=sparkBigDlInitializer.initialize("Test")
      val questionInitializer=new QuestionsInitializer(sc) 
      val lines = sc.textFile("/home/mhd/Desktop/ARCANA Resources/glove.6B/glove.6B.50d.txt")
      val questionsWithoutCleaning=sc.textFile("/home/mhd/Desktop/Data Set/TestNow.txt")
      val questions = questionsWithoutCleaning.map(questionInitializer.clean)
      val orderedQuestions=questions.zipWithIndex().map{case(line,i) => i.toString+" "+line}
      val vectorizationDelegator=new VectorizationDelegator(sc,50)
      val parsedLines = lines.map(vectorizationDelegator.ParseVecGlov)
      val parsedQuestions=orderedQuestions.flatMap(questionInitializer.parseQuestion)
      val result= parsedQuestions.join(parsedLines)
      val resultTest= result.map{case(a,b)=>b}
      val groupedResultTest=resultTest.groupBy(x=>x._1._1)
      val questionTensorTransformer=new QuestionTensorTransformer(sc,questionInitializer.calculateLongestWordsSeq(questions),50)
      val great=groupedResultTest.map(questionTensorTransformer.transform)
      val answer=great.collect()
      answer.foreach{println("---------------StART---------------------")
                     x=>println(x)
                     println("----------------END----------------------")}
  }
}