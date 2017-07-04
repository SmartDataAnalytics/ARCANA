package tech.sda.arcana.spark.profiling
import io.circe.syntax._

import com.mongodb.spark._
import com.mongodb.spark.config._
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
  
  def writeToMongoDB(word:String,rank:String,rsclist:List[String]){
      val rsc = rsclist.asJson
      val x = s"""{"word":"$word","rank":"$rank","rsc":$rsc}"""
      val doc = Document.parse(x)
      val documents = sc.parallelize(Seq(doc))
      MongoSpark.save(documents) 
  }  
  
  def writeChunkToMongoDB(){
    val docs = """
      {"name": "Bilbo Baggins", "age": 50}
      {"name": "Gandalf", "age": 1000}
      {"name": "Thorin", "age": 195}
      {"name": "Balin", "age": 178}
      {"name": "Kíli", "age": 77}
      {"name": "Dwalin", "age": 169}
      {"name": "Óin", "age": 167}
      {"name": "Glóin", "age": 158}
      {"name": "Fíli", "age": 82}
      {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    spark.sparkContext.parallelize(docs.map(Document.parse)).saveToMongoDB()
  }
    
  def main(args: Array[String]) = {
    
    //| Check your session Configurations 
    //> showConfigMap()
    
    //| Write to the DB 
    //> writeToMongoDB("ALIroops","3",List[String]("http://dbpedia.org/resource/Territorial_Troops1", "http://dbpedia.org/resource/Territorial_Troops2", "http://dbpedia.org/resource/Territorial_Troops3","http://dbpedia.org/resource/Territorial_Troops4","http://dbpedia.org/resource/Territorial_Troops5","http://dbpedia.org/resource/Territorial_Troops6","http://dbpedia.org/resource/Territorial_Troops7"))
     
    //> writeChunkToMongoDB()
    
    /*
    val characters = MongoSpark.load(spark)
    characters.createOrReplaceTempView("militaryColl")
    
    val military = spark.sql("SELECT * FROM militaryColl")
    military.show()
    MongoSpark.save(military.write.option("collection", "militaryColl").mode("overwrite"))
    
    println("Reading from the 'military' collection:")
    MongoSpark.load(spark, ReadConfig(Map("collection" -> "military"), Some(ReadConfig(spark)))).show()  
    */
    
    // Work on Updating a value  
    
    
    println("===================CLOSING===================") 
    spark.stop()
  }
}

