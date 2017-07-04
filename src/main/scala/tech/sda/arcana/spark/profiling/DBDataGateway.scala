package tech.sda.arcana.spark.profiling
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
  def main(args: Array[String]) = {
    println("===================X===================")
    println("===================")
    println("|        DB       |")
    println("===================")
    
    val inputUri = "spark.mongodb.input.uri"  
    val outputUri = "spark.mongodb.output.uri"
    val conn = new DBConf()  
    
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config(inputUri, conn.host + conn.dbName + "." + conn.defaultCollection)
      .config(outputUri, conn.host + conn.dbName + "." + conn.defaultCollection)
      //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .getOrCreate()
    
    val sc = spark.sparkContext
    //println(sc)
    
    println("===================Sc===================")
    val configMap:Map[String, String] = spark.conf.getAll
    println(configMap)
    println("===================Writing===================")
    
  
    val doc = Document.parse(s"""{"word":"ALIroops","rank":"3","rsc":["http://dbpedia.org/resource/Territorial_Troops1","http://dbpedia.org/resource/Territorial_Troops2"]}""")
    val documents = sc.parallelize(Seq(doc))
    //val documents = sc.parallelize((1 to 2).map(i => Document.parse(s"""{"word":"sroops","rank":"3","rsc":["http://dbpedia.org/resource/Territorial_Troops1","http://dbpedia.org/resource/Territorial_Troops2"]}""")))    
    
    MongoSpark.save(documents)          
    println("===================Closing Writing===================")
    println("===================LOADING===================")
    val rdd = MongoSpark.load(spark)
    //val rdd = sc.loadFromMongoDB(ReadConfig(Map("spark.mongodb.input.uri" -> "mongodb://127.0.0.1/myDBN.military")))

    println("===================RDD===================")
    /*
    rdd.printSchema() //show schema
    rdd.createOrReplaceTempView("info")
    rdd.select("word").show()
    val Ds=spark.sql("select rsc from info")
    val results = Ds.collect()
    results.foreach(println)
    */
    println("===================DF===================") 
    val myDF=rdd.toDF()
    myDF.show()
    myDF.select("word").show()
    myDF.filter(myDF("rank")<4).show
    //myDF.groupBy("rank").count().show()
    myDF.select(myDF("rsc"), myDF("rank")+10).show()
    
     
     println("===================Collection2===================") 
    //spark.conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/myDBN.neuclear")
    // spark.conf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/myDBN.neuclear")
    //val configMap2:Map[String, String] = spark.conf.getAll
    //println(configMap2)
    //spark.sparkContext.loadFromMongoDB() // Uses the SparkConf for configuration
    //spark.sparkContext.loadFromMongoDB(ReadConfig(Map("spark.mongodb.output.uri" -> "myDBN.military"))) // Uses the ReadConfig
    println("===================ChangeCollection===================")
    val rdd2 = sc.loadFromMongoDB(ReadConfig(Map("spark.mongodb.input.uri" -> "mongodb://127.0.0.1/myDBN.neuclear")))
    rdd2.toDF().select("word").show()
    

    println("===================CLOSING===================") 
    spark.stop()
  }
}

