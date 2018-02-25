package tech.sda.arcana.spark
import tech.sda.arcana.spark.profiling.Dataset2Vec
import tech.sda.arcana.spark.profiling.Word2VecModelMaker
import tech.sda.arcana.spark.profiling.RDFApp
import org.apache.spark.sql.SparkSession
import tech.sda.arcana.spark.profiling.AppDBM
import tech.sda.arcana.spark.profiling.AppConf
import tech.sda.arcana.spark.profiling.SentiWord
import tech.sda.arcana.spark.profiling.ProcessQuestion
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
    val path = "/home/elievex/Repository/resources/"

    // Read the Questions
    val noEmptyRDD=ProcessQuestion.readQuestions(path+AppConf.Questions)
    
    // val DF1 = AppDBM.readDBCollection(AppConf.firstPhaseCollection)
    // val DF2 = AppDBM.readDBCollection(AppConf.secondPhaseCollection)
    // RDFApp.readProcessedData(path+AppConf.processedDatafake)
    // Mapping the questions for processing; Note that I have here functions <DBpedia & DBS> that pass Dataframes to the mapping process 
    // val ds= noEmptyRDD.map(t=>ProcessQuestion.processQuestion(t,path)).toDS().cache()
    val test = RDFApp.readProcessedData(path+AppConf.processedDatafake)
    val ds= noEmptyRDD.map(t=>ProcessQuestion.processQuestion(t,path,test)).toDS().cache()
    ds.show(false)
    
    println("~Processing is done~")
    spark.stop()
  }
}