package tech.sda.arcana.spark.profiling
import com.mongodb.spark._
import org.apache.spark.sql.SparkSession
import com.mongodb
import org.bson.Document
import com.mongodb.spark.config._
import org.apache.spark.SparkContext
import play.api.libs.json._
import org.bson.Document
import scala.util.parsing.json._

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
    
    
    val result = JSON.parseFull("""
      {"name": "Naoki",  "lang": ["Java", "Scala"]}
    """)
    
    var reso="""
      {"name": "Naoki",  "lang": ["Java", "Scala"]}
    """
    /*
    val jsonStr = 
    "{word:" +"\"troops\","+
    "rank:"+  "\"4\","+
    "rsc:[" +
        "\"http://dbpedia.org/resource/Territorial_Troops1\"," +
        "\"http://dbpedia.org/resource/Territorial_Troops2\"," +
        "\"http://dbpedia.org/resource/Territorial_Troops3\""+
    "]}"
    */
    
    //val jsonStr = """{"word":"sroops","rank":"3","rsc":["http://dbpedia.org/resource/Territorial_Troops1","http://dbpedia.org/resource/Territorial_Troops2"]}"""    
            
    //println(jsonStr)

    //val TRDD = sc.parallelize(result:: Nil)
    //val JSRDD = sc.parallelize(Seq(jsonStr))
    """{"name": "Naoki",  "lang": ["Java", "Scala"]}"""
    

    val jsonStr = """{ "metadata": { "key": 84896, "value": 54 }}"""
    val rddw = sc.parallelize(Seq(jsonStr))
  //  val documents = Document.parse(reso)
    //val rddw = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")))
    
    val documents = sc.parallelize((1 to 2).map(i => Document.parse(s"""{"word":"sroops","rank":"3","rsc":["http://dbpedia.org/resource/Territorial_Troops1","http://dbpedia.org/resource/Territorial_Troops2"]}""")))    
    
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
