package tech.sda.arcana.spark.profiling
import java.net.URI
import java.lang.Object
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
    
  def mapperRDF(line:String): Triple = {
    
    //splitting a comma-separated string but ignoring commas in quotes
    val fields = line.split(""" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)""")
  
     // Clean the Subject
     fields(0)=fields(0).stripPrefix("<").stripSuffix(">").trim 
     val path = (new URI(fields(0))).getPath();
     fields(0) = path.substring(path.lastIndexOf('/') + 1);

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
    triples.select("Subject").foreach(println(_))
    //triples.select("Object").show()
    //triples.select("Object").show()

   println("Closing")
   spark.stop()
  }

}

/*

     val uri = new URI("http://commons.dbpedia.org/resource/File:Hunebed_003.jpg");
     val path = uri.getPath();
     val idStr = path.substring(path.lastIndexOf('/') + 1);
     println(idStr)

*/