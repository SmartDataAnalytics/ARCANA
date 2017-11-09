package tech.sda.arcana.spark
import tech.sda.arcana.spark.profiling.Dataset2Vec
import tech.sda.arcana.spark.profiling.Word2VecModelMaker
import tech.sda.arcana.spark.profiling.RDFApp
import org.apache.spark.sql.SparkSession
import tech.sda.arcana.spark.profiling.AppDBM

object ExecuteOperations {
  
  def main(args: Array[String]) = {
        
    val fileName = if (args.length == 0) "src/main/resources/rdf2.nt" else args(0) // or use System.exit(0);
 
    // Read Data & Clean it   
    val RDFDs=RDFApp.importingData(fileName)
    // Prepare Data for the Word2Vec Model  
    Dataset2Vec.MakeWord2VecModel(RDFDs)
    // Create the Word2Vec Model
    Word2VecModelMaker.MakeWord2VecModel()
    // Build MongoDB Data
    AppDBM.buildDB(RDFDs)
  
  }
}