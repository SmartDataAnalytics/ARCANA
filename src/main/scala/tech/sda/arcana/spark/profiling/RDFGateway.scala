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
import org.apache.spark.sql
import scala.util.matching
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

/*
 * An Object that Deals with the RDF Data and parse it 
 * Still need to think of a way to a better representation that include a word and its URI 
 */
 
object RDFApp {
  //rdf subject predicate object
  case class Triple(Subject:String, Predicate:String, Object:String)
    
  val spark = SparkSession.builder()
      .master("local")
      .appName("RDFApp")
      .master("local[*]")
      .getOrCreate()
    
   import spark.implicits._ 
  
  
  //Cleaning the subject (applies to predicate as well)
  def SP_Transform(entity:String): String={
    val newEntity=entity.stripPrefix("<").stripSuffix(">").trim  
    val path = (new URI(newEntity)).getPath();
    return path.substring(path.lastIndexOf('/') + 1);
  }
  
  //CLeaning the object 
  def O_Transform(entity:String): String={
    if(entity.take(1)=="<"){
      val newEntity=entity.stripPrefix("<").stripSuffix(">").trim  
      val path = (new URI(newEntity)).getPath();
      return path.substring(path.lastIndexOf('/') + 1);
    }else if(entity.take(1)=="\""){
    //  return entity.stripPrefix('"').stripSuffix('"').trim;
      return entity.split("\"")(1)
    }
    else{
      // non
      return entity
    }
  }
  
  // A mapper that cleans the data 
  def mapperRDF(line:String): Triple = {
    
    //splitting a comma-separated string but ignoring commas in quotes
    val fields = line.split(""" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)""")
  
    fields(0)=SP_Transform(fields(0))
    fields(1)=SP_Transform(fields(1))
    fields(2)=O_Transform(fields(2))
    
    val triple:Triple = Triple(fields(0), fields(1), fields(2))
    return triple
  }

  // A mapper that provides no data cleaning 
  def basicMapperRDF(line:String): Triple = {
    
    //splitting a comma-separated string but ignoring commas in quotes
    val fields = line.split(""" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)""")

    val triple:Triple = Triple(fields(0), fields(1), fields(2))
    return triple
  }
  
  // Read a file or files and convert them to a dataset after cleaning the content
  def dataToDataset(input: String) = {
    val rawDF = spark.sparkContext.textFile(input) 
    // Remove empty rows 
    val newRDD = rawDF.filter(x => (x != null) && (x.length > 0))
    newRDD.map(basicMapperRDF).toDS().cache()  
  }
    //triples.select("Object").foreach(println(_))
    //triples.select("Object").show()
    //println(triples.count())
  
  def main(args: Array[String]) = {
  
    println("============================")
    println("|        RDF Gateway       |")
    println("============================")
    val input1 = "src/main/resources/rdf.nt" //Single File
    val input2 = "src/main/resources/ntTest/*" //Set of Files
   
    val triples = dataToDataset(input2)
    //> triples.show(false)

    triples.createOrReplaceTempView("triples2")
    //> RLIKE for regular expressions
    val teenagersDF = spark.sql("SELECT * from triples2 where Subject like '%Hunebed%'")
    teenagersDF.show(false)

    println("~Ending Session~")
    spark.stop()
  }
}