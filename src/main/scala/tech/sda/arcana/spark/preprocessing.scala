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
      .appName("ProfilingPreprocessing")
      .master("local[*]")
      .getOrCreate()
  def main(args: Array[String]) = {
    //| should be entered like this val path = "/xx/resources/"    
    //> val path = if (args.length == 0) "src/main/resources/rdf2.nt" else args(0) // or use System.exit(0);
    val path = "/home/elievex/Repository/resources/"
    //| 1 for category, 2 for dataset
    val Word2VecDataType = 1 
    
    //| STEP_1: Read Data & Clean and transform it   
    //> val RDFDs=RDFApp.importingData(path) 
    
    //| STEP_2: Prepare Data for the Word2Vec Model  
    //> Dataset2Vec.MakeWord2VecData(path,Word2VecDataType) // 447.393016719 <=> 519.455495196
    
    //| STEP_3: Create the Word2Vec Model
    //> Word2VecModelMaker.MakeWord2VecModel(path,Word2VecDataType)
    
    //| STEP_4: Create the SentiWord Data
    //> SentiWord.writeProcessedSentiWord(path)

    //| STEP_5: Build URI Data
    //> val RDFDs=RDFApp.readProcessedData(path+AppConf.processedDBpedia)
    //> AppDBM.buildCategoriesDB(RDFDs,path)
    
    //| STEP_6: Set up Expression Collection
    AppDBM.buildExpressionsDB(path)

    println("~Preprocessing is done~")
    spark.stop()
  }
}
// -> Making dataset/dataframe loadable from file in word2vec maker
// -> try map instead of categories in DBgateway 


/* Measure Time:
 *  val t1 = System.nanoTime
    // Task
    val duration = (System.nanoTime - t1) / 1e9d
    println("Duration of Task-processing is:"+duration)
 */

