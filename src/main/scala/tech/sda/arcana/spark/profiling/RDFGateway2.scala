package tech.sda.arcana.spark.profiling

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession;

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
/*
    val triplesRDD = NTripleReader.load(sparkSession, new File(input))

    triplesRDD.take(5).foreach(println(_))
    
    //Triples filtered by subject ( "http://dbpedia.org/resource/Charles_Dickens" )
println("All triples related to Dickens:\n" + graph.find(URI("http://dbpedia.org/resource/Charles_Dickens"), ANY, ANY).collect().mkString("\n"))
 
//Triples filtered by predicate ( "http://dbpedia.org/ontology/influenced" )
println("All triples for predicate influenced:\n" + graph.find(ANY, URI("http://dbpedia.org/ontology/influenced"), ANY).collect().mkString("\n"))
 
//Triples filtered by object ( <http://dbpedia.org/resource/Henry_James> )
println("All triples influenced by Henry_James:\n" + graph.find(ANY, ANY, URI("<http://dbpedia.org/resource/Henry_James>")).collect().mkString("\n"))
  */  
    sparkSession.stop
  }

}