package tech.sda.arcana.spark
import tech.sda.arcana.spark.profiling.Dataset2Vec
import tech.sda.arcana.spark.profiling.Word2VecModelMaker
import tech.sda.arcana.spark.profiling.RDFApp
import org.apache.spark.sql.SparkSession
import tech.sda.arcana.spark.profiling.AppDBM
import tech.sda.arcana.spark.profiling.AppConf
import tech.sda.arcana.spark.profiling.SentiWord
object processing {
      val spark = SparkSession.builder()
      .config(AppConf.inputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
      .config(AppConf.outputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
      .appName("ProfilingProcessing")
      .master("local[*]")
      .getOrCreate()
  def main(args: Array[String]) = {
    //| should be entered like this val path = "/xx/resources/"    
    //> val path = if (args.length == 0) "src/main/resources/rdf2.nt" else args(0) // or use System.exit(0);
    val path = "/home/elievex/Repository/resources/"

    // ProcessQuestions()

    println("~Processing is done~")
    spark.stop()
  }
}