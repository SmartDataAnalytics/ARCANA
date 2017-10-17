package tech.sda.arcana.spark.profiling

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader


import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.pipeline.{ Annotation, StanfordCoreNLP }
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentClass
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations

/*
 * Entry Point that should contain the triggering action 
 * For example trigger answering a question, or trigger filling the database etc 
 */
object App {

  def main(args: Array[String]) = {

    println("==========================")
    println("|        Profiling       |")
    println("==========================")
/*
    val category = new Category()    
    for ( x <- category.categories ) {
         println( x )
      }
  */  
  //  val pt =WSAPI(ws: WSClient);
  // val inst: WSAPI = new WSAPI()
  // inst.Operate()
    //val Junit = new JWI();
    //val x = new WSAPI();
    
		//Junit.printz();
    
    
    //val x = new RDFApp()
   //RDFApp.main(args)
  
  }
}
