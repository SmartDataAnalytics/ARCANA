package tech.sda.arcana.spark.profiling

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession;
import java.net.{URI => JavaURI}
import scala.collection.mutable
object RDFGatewayApp {
  //rdf subject predicate object
  case class Triple(Subject:String, Predicate:String, Object:String)
    
  def mapperRDF(line:String): Triple = {
    val fields = line.split(" ")  
    
    val triple:Triple = Triple(fields(0), fields(1), fields(2))
    return triple
  }
  
  
  def main(args: Array[String]) = {
  
    println("============================")
    println("|        RDF Gateway       |")
    println("============================")
    val input = "src/main/resources/rdf.nt"
    
     val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example (" + input + ")")
      .getOrCreate()
      
      val ops = JenaSparkRDDOps(sparkSession.sparkContext)
      import ops._
   // val triplesRDD = NTripleReader.load(sparkSession, new File(input))

    //triplesRDD.take(5).foreach(println(_))
     
    val triplesRDD = NTripleReader.load(sparkSession, new File(input))

    
    val graph: TripleRDD = triplesRDD
    //Triples filtered by subject ( "http://dbpedia.org/resource/Charles_Dickens" )
    // <http://commons.dbpedia.org/resource/Category:Public_domain>
    println("All triples related to File:Lijn10:\n" + graph.find(URI("http://commons.dbpedia.org/resource/File:Lijn10.jpg"), ANY, ANY).collect().mkString("\n"))
     /*
    //Triples filtered by predicate ( "http://dbpedia.org/ontology/influenced" )
    println("All triples for predicate influenced:\n" + graph.find(ANY, URI("http://dbpedia.org/ontology/influenced"), ANY).collect().mkString("\n"))
     
    //Triples filtered by object ( <http://dbpedia.org/resource/Henry_James> )
    println("All triples influenced by Henry_James:\n" + graph.find(ANY, ANY, URI("<http://dbpedia.org/resource/Henry_James>")).collect().mkString("\n"))
      */  
    
    
    
    println("_.isLiteral()")
    val objects = graph.filterSubjects(_.isLiteral()).collect.mkString("\n")
    
    println(objects)
    
    
    
    println("Stopping the session")
    sparkSession.stop
  }

}