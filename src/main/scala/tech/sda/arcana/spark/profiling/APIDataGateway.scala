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
object APIData {
  
  def main(args: Array[String]) = {
  
    println("============================")
    println("|        API Gateway       |")
    println("============================")
    val input = "src/main/resources/rdf.nt"
    
     val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example (" + input + ")")
      .getOrCreate()
      

    sparkSession.stop
  }

}