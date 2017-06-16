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
    
    val spark = SparkSession.builder()
      .master("local")
      .appName("RDFApp")
      .master("local[*]")
      .getOrCreate()
    
   import spark.implicits._ 
    val lines = spark.sparkContext.textFile(input)
    val triples = lines.map(mapperRDF).toDS().cache()

   triples.select("Object").show()

   spark.stop()
  }

}