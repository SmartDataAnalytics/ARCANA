package tech.sda.arcana.spark.profiling
import org.apache.spark.sql.functions.{ min, max }
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import io.circe.syntax._
import scala.collection.JavaConverters._
import com.mongodb.spark._
import com.mongodb.spark.config._
import scala.collection.mutable.ArrayBuffer
import com.mongodb
import org.bson.Document
import com.mongodb.spark.config._
import org.apache.spark.SparkContext
import scala.util.parsing.json._
import org.bson.types.ObjectId
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2VecModel
/*
 * An Object that is responsible for the interaction with MongoDB to store and read data
 */
object AppDBM {

  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnector")
    .config(AppConf.inputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
    .config(AppConf.outputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
    //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse") >> Windows
    .getOrCreate()

  val sc = spark.sparkContext

  //| Check your session Configurations
  def showConfigMap() {
    val configMap: Map[String, String] = spark.conf.getAll
    println(configMap)
  }

  //| Write to the DB
  def writeToMongoDB(word: String, rank: String, rsclist: List[String]) {
    val rsc = rsclist.asJson
    val x = s"""{"word":"$word","rank":"$rank","rsc":$rsc}"""
    val doc = Document.parse(x)
    val documents = sc.parallelize(Seq(doc))
    MongoSpark.save(documents)
  }
  def writeFormedChunkToMongoDB(buffer: String, collection: String) {
    println("S2")
    println(buffer)
    val docs = buffer.trim.stripMargin.split("@#@").toSeq
    println(docs)
    println("S3")
    sc.parallelize(docs.map(Document.parse)).saveToMongoDB(WriteConfig(Map("uri" -> s"mongodb://127.0.0.1/myDBN.$collection")))
  }

  def formRecord(id: Integer, word: String, rank: Double, rsclist: List[String]): String = {
    val rsc = rsclist.asJson
    val x = s"""{"_id":$id,"word":"$word","rank":$rank,"rsc":$rsc}"""
    x
  }

  // Using the SQL helpers and StructFields helpers
  def writeRecordToDB() {
    val objectId = "123400000000000000000000"
    val newDocs = Seq(new Document("_id", new ObjectId(objectId)).append("a", 1), new Document("_id", new ObjectId()).append("a", 2))
    MongoSpark.save(sc.parallelize(newDocs))
    /*
       * val documents = sc.parallelize(
       *  Seq(new Document("fruits", List("apples", "oranges", "pears").asJava))
       * )
       */
  }

  def ChangeCollection() {
    val characters = MongoSpark.load(spark)
    characters.createOrReplaceTempView("characters")

    val centenarians = spark.sql("SELECT name, age FROM characters WHERE age >= 100")
    centenarians.show()

    MongoSpark.save(centenarians.write.option("collection", "hundredClub").mode("overwrite")) // Append or overwrite <overwrite is buggy when the collection already exists>

    println("Reading from the 'hundredClub' collection:")
    MongoSpark.load(spark, ReadConfig(Map("collection" -> "hundredClub"), Some(ReadConfig(spark)))).show()
  }

  def FetchMaxId(collection: String): Int = {

    val rdd2 = sc.loadFromMongoDB(ReadConfig(Map("spark.mongodb.input.uri" -> s"mongodb://127.0.0.1/ArcanaDB.$collection")))
    rdd2.toDF().createOrReplaceTempView("DB")
    val maxID = spark.sql("SELECT max(cast(_id as int)) FROM DB")

    maxID.collect()(0).getInt(0)
  }
  def readCollection(collection: String) {

    val rdd2 = sc.loadFromMongoDB(ReadConfig(Map("spark.mongodb.input.uri" -> s"mongodb://127.0.0.1/myDBN.$collection")))
    rdd2.toDF().createOrReplaceTempView("DB")
    rdd2.toDF().show
    val word = "nuclearbomb"
    val res = spark.sql(s"SELECT rsc FROM DB where word = '$word' ")
    res.show
    res.collect().foreach(println)
    //for (e <- res) println(e)
  }
   def readDBCollection(collection: String):DataFrame= {
     MongoSpark.load(spark, ReadConfig(Map("collection" -> collection), Some(ReadConfig(spark))))
   }
  def EnterSchemaData() {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var z = Array[Integer](8, 8, 8)

    val days = List(1, 2, 3)

    val theRow = Row(33, "tito", days, Array[Double](1.3, 1.3, 1.3))
    val theRow2 = Row(31, "dima", List(9, 9, 9), Array[Double](1.3, 1.3, 1.3))
    //val theRow2 =Row(7,"dima",Array[java.lang.Integer](9,9,9), Array[Double](1.3,1.3,1.3))
    val theRdd = sc.makeRDD(Array(theRow, theRow2))

    val df = theRdd.map {
      case Row(s0, s1, s2, s3) => X(s0.asInstanceOf[Int], s1.asInstanceOf[String], s2.asInstanceOf[List[Integer]], s3.asInstanceOf[Array[Double]])
    }.toDF()
    df.show()

    //military.show()
    MongoSpark.save(df.write.option("collection", "testcase").mode("append"))
  }
  // Clean the subject and get back what it talks about
  def getExpFromSubject(Subject: String): String = {
    var temp = (Subject.substring(Subject.lastIndexOf('/') + 1)).replaceAll(">", "")
    temp = if (temp contains ':') temp.substring(temp.lastIndexOf(':') + 1) else temp
    temp = if (temp contains '_') temp.replaceAll("_", " ") else temp
    temp.toLowerCase
  }

  def buildExpressionsDB(path:String){
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    var DBRows = ArrayBuffer[Row]()
    var rlID = 0 
    for(t<-AppConf.tuples){
       rlID += 1
       DBRows += Row(t._1,t._2,rlID)
       WordNet.getSynsets(t._1,path).foreach(x=>DBRows += Row(x,t._2,rlID))
       WordNet.getSynsets(t._2,path).foreach(x=>DBRows += Row(t._1,x,rlID))
    }
 
    val dbRdd = sc.makeRDD(DBRows)
    val df = dbRdd.map {
      case Row(s0, s1, s2) => Expression(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[Int])
    }.toDF()
    
    MongoSpark.save(df.write.option("collection", AppConf.secondPhaseCollection).mode("overwrite"))//Accepted save modes are 'overwrite', 'append', 'ignore', 'error'.
    //println(DBRows.size)
    println("~Expressions Collection is created~")
  }
  
   // get Sentiment analysis for the word as N, V, R, A and if non was fetched then grab the Uclassify score too
   def getSentiScores(word:String,sentiDF:DataFrame):Array[String]={
        var resultneg="-9"
        val sentiPosScore= SentiWord.getSentiScoreForAllPOS(word,sentiDF)  // 5 vals
        if(sentiPosScore(0)=="-9" && sentiPosScore(1)=="-9" && sentiPosScore(2)=="-9" && sentiPosScore(3)=="-9")
        {
          val result = APIData.getRankUclassify(word)
          resultneg = result._1
        }
        sentiPosScore(4) = resultneg
        sentiPosScore
   }
  // Build the Database with resources
  def operateOnDB(DF: DataFrame, modelvec: Word2VecModel,sentiDF:DataFrame,path:String) {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val categories = AppConf.categories
    var DBRows = ArrayBuffer[Row]()
    
    for (x <- categories) {
      val myUriList = Dataset2Vec.fetchAllOfWordAsSubject(DF.toDF(), x)
      //val sentiPosScore = getSentiScores(x,sentiDF)        
      for (y <- myUriList) {
        val uriSentiPosScore = getSentiScores(AppDBM.getExpFromSubject(y.Uri),sentiDF)
        DBRows += Row(y.Uri, AppDBM.getExpFromSubject(y.Uri), x,uriSentiPosScore(0).toDouble,uriSentiPosScore(1).toDouble,uriSentiPosScore(2).toDouble,uriSentiPosScore(3).toDouble, uriSentiPosScore(4).toDouble, "", 0.0)
        // Word2Vec Synonyms
        var synSet=Word2VecModelMaker.getWord2VecSynonyms(modelvec,y.Uri)
        synSet.foreach{syn=>
          val synSentiPosScore = getSentiScores(AppDBM.getExpFromSubject(syn.word),sentiDF)
          DBRows += Row(syn.word, AppDBM.getExpFromSubject(syn.word), x,synSentiPosScore(0).toDouble,synSentiPosScore(1).toDouble,synSentiPosScore(2).toDouble,synSentiPosScore(3).toDouble,synSentiPosScore(4).toDouble,y.Uri, syn.similarity)
        }
       }
    }
    val dbRdd = sc.makeRDD(DBRows)
    val df = dbRdd.map {
    case Row(s0, s1, s2, s3, s4, s5, s6, s7, s8, s9) => DBRecord(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[String], s3.asInstanceOf[Double], s4.asInstanceOf[Double], s5.asInstanceOf[Double], s6.asInstanceOf[Double],s7.asInstanceOf[Double],s8.asInstanceOf[String],s9.asInstanceOf[Double])
    }.toDF()
    MongoSpark.save(df.write.option("collection", AppConf.firstPhaseCollection).mode("overwrite"))
    println("~Categories Collection is created~")
  }

  def buildCategoriesDB(DS: DataFrame,path:String){
    import spark.implicits._
    val Word2VecModel = Word2VecModelMaker.loadWord2VecModel(path+AppConf.Word2VecModel)
    val sentiDF = SentiWord.readProcessedSentiWord(path)
    operateOnDB(DS,Word2VecModel,sentiDF,path)
  }
  
  def main(args: Array[String]) = {
    
    //> showConfigMap()

    //>expressionsDB()

    //> writeToMongoDB("ALIroops","3",List[String]("http://dbpedia.org/resource/Territorial_Troops1", "http://dbpedia.org/resource/Territorial_Troops2", "http://dbpedia.org/resource/Territorial_Troops3","http://dbpedia.org/resource/Territorial_Troops4","http://dbpedia.org/resource/Territorial_Troops5","http://dbpedia.org/resource/Territorial_Troops6","http://dbpedia.org/resource/Territorial_Troops7"))

    /////////////// READING
    /*
    val rdd =  MongoSpark.load(spark)

      println(rdd.count)
    //  println(rdd.first.toJson)

    rdd.createOrReplaceTempView("mongoDB")

    val centenarians = spark.sql("SELECT * FROM mongoDB where ")

    val newT = spark.sql("update mongoDB set weight = '10' where expression = 'nuclearA1' ")

    newT.show()
	  */

    //> writeChunkToMongoDB()

    //> EnterSchemaData()
    //> println(FetchMaxId("testcase"))

    //Writing a chunktto Mongo
    //> writeChunkToMongoDB("ChunkCase")

    println("===================CLOSING===================")
    spark.stop()
  }
}
    /* DONT DELETE Same results using Foreach
       categories.foreach{x=>
       val myUriList = Dataset2Vec.fetchAllOfWordAsSubject(RDFDs.toDF(), x)
       val sentiPosScore = getSentiScores(x.toLowerCase,sentiDF) 
       myUriList.foreach{y=>
         DBRows += Row(y.Uri, AppDBM.getExpFromSubject(y.Uri), x,sentiPosScore(0),sentiPosScore(1),sentiPosScore(2),sentiPosScore(3), sentiPosScore(4), "", "")
         
          try{
            val synonyms = modelvec.findSynonyms(y.Uri, 1000)
            val synResult = synonyms.filter("similarity>=0.3").as[Synonym].collect
            synResult.foreach{synonym=>
              val synSentiPosScore = getSentiScores(AppDBM.getExpFromSubject(synonym.word),sentiDF)
              DBRows += Row(synonym.word, AppDBM.getExpFromSubject(synonym.word), x,synSentiPosScore(0),synSentiPosScore(1),synSentiPosScore(2),synSentiPosScore(3),synSentiPosScore(4),y.Uri, synonym.similarity)
            }
          } 
          catch{
            case e: Exception => println("didn't find synonyms for: "+y.Uri)
          }
          
          val dbRdd = sc.makeRDD(DBRows)
          val df = dbRdd.map {
          case Row(s0, s1, s2, s3, s4, s5, s6,s7,s8,s9) => DBRecord(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[String], s3.asInstanceOf[Double], s4.asInstanceOf[Double], s5.asInstanceOf[Double], s6.asInstanceOf[Double],s7.asInstanceOf[Double],s8.asInstanceOf[String],s9.asInstanceOf[Double])
          }.toDF()
          MongoSpark.save(df.write.option("collection", AppConf.firstPhaseCollection).mode("append"))
         }
       }
     */
