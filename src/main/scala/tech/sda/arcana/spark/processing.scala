package tech.sda.arcana.spark
import tech.sda.arcana.spark.profiling.Dataset2Vec
import tech.sda.arcana.spark.profiling.Word2VecModelMaker
import tech.sda.arcana.spark.profiling.RDFApp
import org.apache.spark.sql.SparkSession
import tech.sda.arcana.spark.profiling.AppDBM
import tech.sda.arcana.spark.profiling.AppConf
import tech.sda.arcana.spark.profiling.SentiWord
import tech.sda.arcana.spark.profiling.ProcessQuestion
import scala.collection.mutable.ListBuffer
import tech.sda.arcana.spark.profiling.QuestionObj
object processing {
      val spark = SparkSession.builder()
      .config(AppConf.inputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
      .config(AppConf.outputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
      .appName("ProfilingProcessing")
      .master("local[*]")
      .getOrCreate()
      import spark.implicits._
  def main(args: Array[String]) = {
    //| should be entered like this val path = "/xx/resources/"    
    //> val path = if (args.length == 0) "src/main/resources/rdf2.nt" else args(0) // or use System.exit(0);
    //val path = "/home/elievex/Repository/resources/"
    val path= args(0)
    // Read the Questions
    val noEmptyRDD=ProcessQuestion.readQuestions(path+AppConf.Questions)
    val t1 = System.nanoTime
  
    // val ds= noEmptyRDD.map(t=>ProcessQuestion.processQuestion(t,path)).toDS().cache()
    // val test = RDFApp.readProcessedData(path+AppConf.processedDBpedia).cache()
    //sc.broadcast(test)
    
    
    // METHOD 1
    /*
    val ds= noEmptyRDD.map(t=>ProcessQuestion.processQuestion(t,path,RDFApp.readProcessedData(path+AppConf.processedDBpedia))).toDS().cache()
    ds.show(false)
    val duration = (System.nanoTime - t1) / 1e9d
    println("Duration of Task-processing is:"+duration)
    */
    // METHOD 2
    val sc=spark.sparkContext
    val sourceRecords = sc.textFile(path+AppConf.StopWords).collect().toList
    val listOfStopwords=sourceRecords
    val DBpedia = RDFApp.readProcessedData(path+AppConf.processedDBpedia).cache()
    val DFDB1 = AppDBM.readDBCollection(AppConf.firstPhaseCollection)
    val DFDB2 = AppDBM.readDBCollection(AppConf.secondPhaseCollection)
    val myList=noEmptyRDD.collect().toList
    var result = new ListBuffer[QuestionObj]()

    myList.foreach{
      f=> 
        result+=ProcessQuestion.processQuestion(f,path,DBpedia,DFDB1,DFDB2)
    }
    println("~Processing is done~")
    val duration = (System.nanoTime - t1) / 1e9d
    println("Duration of Task-processing is:"+duration)
    result.foreach{
      x=>println(x.sentence+"::"+x.phaseTwoScore+"::"+x.summary)
    }
    spark.stop()
  }
}
/*
    val test = RDFApp.readProcessedData(path+AppConf.processedDBpedia).cache()
    val myList=noEmptyRDD.collect().toList
    myList.foreach{
      f=>ProcessQuestion.processQuestion(f,path,test)
      }
*/
