package tech.sda.arcana.spark
import tech.sda.arcana.spark.profiling.Dataset2Vec
import tech.sda.arcana.spark.profiling.Word2VecModelMaker
import tech.sda.arcana.spark.profiling.RDFApp
import org.apache.spark.sql.SparkSession
import tech.sda.arcana.spark.profiling.AppDBM
import tech.sda.arcana.spark.profiling.AppConf

/*
 * Entry Point that should contain the triggering action 
 * For example trigger answering a question, or trigger filling the database etc 
 */
object ExecuteOperations {
  
  def main(args: Array[String]) = {
    //| should be entered like this val path = "/xx/resources/"    
    //> val path = if (args.length == 0) "src/main/resources/rdf2.nt" else args(0) // or use System.exit(0);
    val path = "/home/elievex/Repository/resources/"
    
    //| Read Data & Clean and transform it   
    val RDFDs=RDFApp.importingData(path+AppConf.dbpedia)

    //| Prepare Data for the Word2Vec Model  
    Dataset2Vec.MakeWord2VecModel(RDFDs,path,1) // 1 for category, 2 for dataset
    println("DON")
    //| Create the Word2Vec Model
    //Word2VecModelMaker.MakeWord2VecModel()
    //| Build MongoDB Data
    //AppDBM.buildDB(RDFDs)
  
  }
}