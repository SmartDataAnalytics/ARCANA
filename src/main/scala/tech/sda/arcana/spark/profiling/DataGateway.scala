package tech.sda.arcana.spark.profiling


import com.mongodb.spark._
import org.apache.spark.sql.SparkSession

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
      .getOrCreate()
      println("===================1") 
    val rdd = MongoSpark.load(spark)
    println("===================2") 
    println(rdd.count)
   // println(rdd.first.toJson)
     println("===================3")  
      
  }
}