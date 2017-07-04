package tech.sda.arcana.spark.profiling
import com.mongodb.spark._
import org.apache.spark.sql.SparkSession

/*
 * An obsolete object to deal with DB
 * I will keep it for now untill I finish implementing the whole connection with the database (It might still have some valid ideas that I don't want to lose
 */
object AppDB {

  def main(args: Array[String]) = {

    println("===================")
    println("|        DB       |")
    println("===================")

    
    
    
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mycustomers.customers?readPreference=primaryPreferred")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mycustomers.customers")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()
      println("===================1") 
    val rdd = MongoSpark.load(spark)
    println("===================2") 
    println(rdd.count)
    //println(rdd.first.toJson)
     println("===================3")  
      
  }
}
