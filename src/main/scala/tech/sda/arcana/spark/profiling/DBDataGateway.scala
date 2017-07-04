package tech.sda.arcana.spark.profiling
//import io.circe.jackson
import io.circe.syntax._


import com.mongodb.spark._
import org.apache.spark.sql.SparkSession
import com.mongodb
import org.bson.Document
import com.mongodb.spark.config._
import org.apache.spark.SparkContext
import org.bson.Document
import scala.util.parsing.json._
/*
 * An Object that is responsible for the interaction with MongoDB to store and read data
 */
object AppDBM {
    
  val inputUri = "spark.mongodb.input.uri"  
  val outputUri = "spark.mongodb.output.uri"
  val conn = new DBConf()  
  
  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config(inputUri, conn.host + conn.dbName + "." + conn.defaultCollection)
    .config(outputUri, conn.host + conn.dbName + "." + conn.defaultCollection)
    //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse") >> Windows
    .getOrCreate()
  
  val sc = spark.sparkContext
  

  def showConfigMap(){
      val configMap:Map[String, String] = spark.conf.getAll
      println(configMap)
  }  
  
  def writeToMongoDB(x:String){
      val doc = Document.parse(x)
      val documents = sc.parallelize(Seq(doc))
      MongoSpark.save(documents) 
  }  
  
  def list2StringList(x: List[String]): String = {
    var Result = ""
    for (ship <- x) {
      Result=Result+'"'+ship+'"'+','
      //println(ship)
      }  
    return Result
  }
    
  def main(args: Array[String]) = {
    println("===================OPEN===================") 
    showConfigMap()
    
    val word = "ALIroops"
    val rank = "3"
    val rsclist  = List[String]("http://dbpedia.org/resource/Territorial_Troops1", "http://dbpedia.org/resource/Territorial_Troops2", "http://dbpedia.org/resource/Territorial_Troops3","http://dbpedia.org/resource/Territorial_Troops4","http://dbpedia.org/resource/Territorial_Troops5","http://dbpedia.org/resource/Territorial_Troops6","http://dbpedia.org/resource/Territorial_Troops7")
    //val rsc=rsclist.mkString("\"", "\",\"", "\"")
    //val test= mapper.writeValueAsString(rsclist)
    val rsc = rsclist.asJson
    println(rsc)
    //writeToMongoDB(s"""{"word":"$word","rank":"$rank","rsc":$rsc}""")
    

    println("===================CLOSING===================") 
    spark.stop()
  }
}

