package tech.sda.arcana.spark.profiling
import com.mongodb.spark._
import org.apache.spark.sql.SparkSession
import com.mongodb
import org.bson.Document
import com.mongodb.spark.config._
object AppDBM {


  def main(args: Array[String]) = {
    println("===================X===================")
    println("===================")
    println("|        DB       |")
    println("===================")
    
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/myDBN.nuclear")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/myDBN.nuclear")
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()
    

    val configMap:Map[String, String] = spark.conf.getAll
    println(configMap)
    println("===================LOADING===================")
    val rdd = MongoSpark.load(spark)

    println("===================RDD===================")
    //rdd.printSchema() //show schema
    rdd.createOrReplaceTempView("info")
    rdd.select("word").show()
    val Ds=spark.sql("select rsc from info where rank = 4 ")
    val results = Ds.collect()
    results.foreach(println)
    
    println("===================DF===================") 
    val myDF=rdd.toDF()
    myDF.show()
    myDF.select("word").show()
    myDF.filter(myDF("rank")<4).show
    //myDF.groupBy("rank").count().show()
    myDF.select(myDF("rsc"), myDF("rank")+10).show()
    
     
     println("===================Collection2===================") 
     spark.conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/myDBN.military")
     spark.conf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/myDBN.military")
    val configMap2:Map[String, String] = spark.conf.getAll
    println(configMap2)
    //spark.sparkContext.loadFromMongoDB() // Uses the SparkConf for configuration
    //spark.sparkContext.loadFromMongoDB(ReadConfig(Map("spark.mongodb.output.uri" -> "myDBN.military"))) // Uses the ReadConfig
    println("===================ter===================")
    val rdd2 = MongoSpark.load(spark)
    rdd2.select("word").show()
    println("===================CLOSING===================") 
    spark.stop()
  }
}
