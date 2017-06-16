package tech.sda.arcana.spark.profiling

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession;


object RDFApp {
  //rdf subject predicate object
  case class Triple(Subject:String, Predicate:String, Object:String)
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapperRDF(line:String): Triple = {
    val fields = line.split(" ")  
    
    val triple:Triple = Triple(fields(0), fields(1), fields(2))
    return triple
  }
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
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
   //people.foreach(println)
   spark.stop()
  }

}