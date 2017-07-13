package tech.sda.arcana.spark.profiling
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.Row
import org.apache.spark.sql._ 
import io.circe.syntax._
import scala.collection.JavaConverters._
import com.mongodb.spark._
import com.mongodb.spark.config._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
import com.mongodb
import org.bson.Document
import com.mongodb.spark.config._
import org.apache.spark.SparkContext
import org.bson.Document
import scala.util.parsing.json._
import org.bson.types.ObjectId
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
  def writeFormedChunkToMongoDB(buffer:String,collection:String){
    println("S2")
      //println(buffer)
      val docs=buffer.trim.stripMargin.split("@#@").toSeq
      println(docs)
      println("S3")
      sc.parallelize(docs.map(Document.parse))saveToMongoDB(WriteConfig(Map("uri" -> s"mongodb://127.0.0.1/myDBN.$collection")))
  }
  def writeChunkToMongoDB(collection:String){
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
      
      println(docs)
      //sc.parallelize(docs.map(Document.parse)).saveToMongoDB()
      
    //  sc.parallelize(docs.map(Document.parse))saveToMongoDB(WriteConfig(Map("uri" -> s"mongodb://127.0.0.1/myDBN.$collection")))
      
      
      //documents.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://example.com/database.collection")))
    

  }
  def formRecord(id:Integer,word:String,rank:Double,rsclist:List[String]): String ={
      val rsc = rsclist.asJson
      val x = s"""{"_id":$id,"word":"$word","rank":$rank,"rsc":$rsc}"""
      x
  }
  
  // Using the SQL helpers and StructFields helpers
  def writeRecordToDB(){
      val objectId = "123400000000000000000000"
      val newDocs = Seq(new Document("_id", new ObjectId(objectId)).append("a", 1), new Document("_id", new ObjectId()).append("a", 2))
      MongoSpark.save(sc.parallelize(newDocs))
      /*
       * val documents = sc.parallelize(
       *  Seq(new Document("fruits", List("apples", "oranges", "pears").asJava))
       * )
       */
  }
  
  def ChangeCollection(){
    val characters = MongoSpark.load(spark)
    characters.createOrReplaceTempView("characters")
    
    val centenarians = spark.sql("SELECT name, age FROM characters WHERE age >= 100")
    centenarians.show()
    
    MongoSpark.save(centenarians.write.option("collection", "hundredClub").mode("overwrite")) // Append or overwrite <overwrite is buggy when the collection already exists>

    println("Reading from the 'hundredClub' collection:")
    MongoSpark.load(spark, ReadConfig(Map("collection" -> "hundredClub"), Some(ReadConfig(spark)))).show()
  }
  
  def FetchMaxId(collection: String) : Int = {

    val rdd2 = sc.loadFromMongoDB(ReadConfig(Map("spark.mongodb.input.uri" -> s"mongodb://127.0.0.1/myDBN.$collection" )))
    rdd2.toDF().createOrReplaceTempView("DB")
    val maxID = spark.sql("SELECT max(cast(_id as int)) FROM DB")

    maxID.collect()(0).getInt(0)   
}
  
  def EnterSchemaData(){
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    var z = Array[Integer](8,8,8)
    
    val days = List(1,2,3)
    
    val theRow =Row(33,"tito",days, Array[Double](1.3,1.3,1.3))
    val theRow2 =Row(31,"dima",List(9,9,9), Array[Double](1.3,1.3,1.3))
    //val theRow2 =Row(7,"dima",Array[java.lang.Integer](9,9,9), Array[Double](1.3,1.3,1.3))
    val theRdd = sc.makeRDD(Array(theRow,theRow2))
    
    val df=theRdd.map{
        case Row(s0,s1,s2,s3)=>X(s0.asInstanceOf[Int],s1.asInstanceOf[String],s2.asInstanceOf[List[Integer]],s3.asInstanceOf[Array[Double]])
        }.toDF()
    df.show()

    //military.show()
    MongoSpark.save(df.write.option("collection", "testcase").mode("append"))
  }
  //Schema 
  case class X(_id: Int,_expression: String,indices: List[Integer], weights: Array[Double] )  
  case class Record(_id: Int, expression: String, rank:Double, rsc: List[String])  
  
  def main(args: Array[String]) = {
    
    //| Check your session Configurations 
    //> showConfigMap()
    
    //| Write to the DB 
    //> writeToMongoDB("ALIroops","3",List[String]("http://dbpedia.org/resource/Territorial_Troops1", "http://dbpedia.org/resource/Territorial_Troops2", "http://dbpedia.org/resource/Territorial_Troops3","http://dbpedia.org/resource/Territorial_Troops4","http://dbpedia.org/resource/Territorial_Troops5","http://dbpedia.org/resource/Territorial_Troops6","http://dbpedia.org/resource/Territorial_Troops7"))
     
    //> writeChunkToMongoDB()
    
    //> EnterSchemaData()
    //> println(FetchMaxId("testcase"))
    
    //Writing a chunktto Mongo
    //> writeChunkToMongoDB("ChunkCase")
    
    

    val buf = new ArrayBuffer[String]()
 
    buf += formRecord(100,"A1",10,List("A1Rsc1","A1Rsc2", "A1Rsc3"))
    buf += "@#@"
    buf += formRecord(101,"A2",20,List("A2Rsc1","A2Rsc2", "A2Rsc3"))
    buf += "@#@"
    buf += formRecord(102,"A3",30,List("A3Rsc1","A3Rsc2", "A3Rsc3"))

    //Solve the abo
    
    println("S1")
    writeFormedChunkToMongoDB(buf.mkString(" "),"ChunkCase")
    //writeChunkToMongoDB("TEST")
    println("===================CLOSING===================") 
    spark.stop()
  }
}

