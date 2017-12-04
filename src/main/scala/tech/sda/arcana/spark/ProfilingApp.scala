package tech.sda.arcana.spark
import tech.sda.arcana.spark.profiling.Dataset2Vec
import tech.sda.arcana.spark.profiling.Word2VecModelMaker
import tech.sda.arcana.spark.profiling.RDFApp
import org.apache.spark.sql.SparkSession
import tech.sda.arcana.spark.profiling.AppDBM
import tech.sda.arcana.spark.profiling.AppConf
import tech.sda.arcana.spark.profiling.SentiWord

/*
 * Entry Point that should contain the triggering action 
 * For example trigger answering a question, or trigger filling the database etc 
 */
object ExecuteOperations {
    val spark = SparkSession.builder()
      .config(AppConf.inputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
      .config(AppConf.outputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
      .appName("ProfilingApp")
      .master("local[*]")
      .getOrCreate()
  def main(args: Array[String]) = {
    //| should be entered like this val path = "/xx/resources/"    
    //> val path = if (args.length == 0) "src/main/resources/rdf2.nt" else args(0) // or use System.exit(0);
    val path = "/home/elievex/Repository/resources/"

    //| Read Data & Clean and transform it   
    val RDFDs=RDFApp.importingData(path+AppConf.dbpedia) 
    println("~RDF data are created~")
    
    //| 1 for category, 2 for dataset
    val Word2VecDataType = 1 
    
    //| Prepare Data for the Word2Vec Model  
    Dataset2Vec.MakeWord2VecData(RDFDs,path,Word2VecDataType)
    
    //| Create the Word2Vec Model
    Word2VecModelMaker.MakeWord2VecModel(path,Word2VecDataType)
    
    //| Create the SentiWord Data
    SentiWord.writeProcessedSentiWord(path)
    
    //| Set up Expression Collection
    AppDBM.buildExpressionsDB(path)

    //| Build URI Data
    //AppDBM.buildCategoriesDB(RDFDs,path)
    
    //| Process Questions
    //XX
    
    spark.stop()
  }
}