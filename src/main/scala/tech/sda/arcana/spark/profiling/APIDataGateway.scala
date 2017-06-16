package tech.sda.arcana.spark.profiling

import java.io._
import scala.util.parsing.json._
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
object APIData {
  
  // 1st way to do it
  @throws(classOf[java.io.IOException])
  def fetch(url: String) = scala.io.Source.fromURL(url).mkString
  
  
  def main(args: Array[String]) = {
  
    println("============================")
    println("|        API Gateway       |")
    println("============================")
    val input = "src/main/resources/rdf.nt"
    
    val result = fetch("http://words.bighugelabs.com/api/2/fe297721a04ca9641ae3a5b1ae3033a2/germany/json")
    val parsed = JSON.parseFull(result)
    println(parsed)
    
  }

}