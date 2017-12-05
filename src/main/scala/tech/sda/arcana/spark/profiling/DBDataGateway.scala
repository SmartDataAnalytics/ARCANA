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

  def writeChunkToMongoDB(collection: String) {
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
      {"name": "Bombur"}"""
    //.trim.stripMargin.split("[\\r\\n]+").toSeq
    //println(docs)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val events = sc.parallelize(docs :: Nil)

    // read it
    val df = sqlContext.read.json(events)

    //df.show

    import sqlContext.implicits._

    MongoSpark.save(df.write.option("collection", "DFClub").mode("append"))
    /*val json: JsValue = Json.parse(docs)


      val rdd = sc.parallelize(jsonStr::Nil);
      var df = sqlContext.read.json(rdd);
      df.printSchema()


    val df = spark.read.format("json").json(docs)
    df.show()
    */

    val j = sc.parallelize(docs)

    //val documents = sc.parallelize(docs.map(Document.parse))
    //val X2 = documents.toDF()

    //val t =  docs.toDF()
    //val df = spark.read.format("json").json()
    //t.show()
    //val X = j.toDF()
    //MongoSpark.save(X.write.option("collection", "DFClub").mode("append"))
    //documents.toJavaRDD()
    //MongoSpark.save(documents)
    //MongoSpark.save(centenarians.write.option("collection", "hundredClub").mode("overwrite"))

    //sc.parallelize(docs.map(Document.parse)).saveToMongoDB()

    //  sc.parallelize(docs.map(Document.parse))saveToMongoDB(WriteConfig(Map("uri" -> s"mongodb://127.0.0.1/myDBN.$collection")))

    //documents.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://example.com/database.collection")))

  }

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
    temp
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

          //DBRows += Row
      //DBRows.foreach(println)
      val dbRdd = sc.makeRDD(DBRows)

    val df = dbRdd.map {
      case Row(s0, s1, s2) => Expression(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[Int])
    }.toDF()
    
    MongoSpark.save(df.write.option("collection", AppConf.secondPhaseCollection).mode("overwrite"))//Accepted save modes are 'overwrite', 'append', 'ignore', 'error'.
    //println(DBRows.size)
    println("~Expressions Collection is created~")
  }
  
  
  // Build the Database with resources
  def operateOnDB(DF: Dataset[Triple], model: Word2VecModel,sentiDF:DataFrame,path:String) {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val categories = AppConf.categories
    var DBRows = ArrayBuffer[Row]()
// DBRecord(_id: Int, uri: String, expression: String, category: String, score: Double, weight: Double, objectOf: String) // score = cosine similary - weight = sentiment analysis
  
    var _idCounter: Int = 0
    for (x <- categories) {
      //| Get different POS scores for category x
      val sentiPosScore= SentiWord.getSentiScoreForAllPOS(x,sentiDF)
      //| get URIS that has the category as a word
      val myUriList = Dataset2Vec.fetchAllOfWordAsSubject(DF.toDF(), x)
     
     for (y <- myUriList) {
        DBRows += Row(_idCounter, y.Uri, getExpFromSubject(y.Uri), x, 0.0, 0.0, "")
        _idCounter += 1
        // Find synonyms to this URI
        val synonyms = model.findSynonyms(y.Uri, 1000)
        val result = synonyms.filter("similarity>0.2").as[Synonym].collect
        for (synonym <- result) {
          DBRows += Row(_idCounter, synonym.word, getExpFromSubject(synonym.word), x, synonym.similarity, 0.0, y.Uri)
          _idCounter += 1
        }
      }
    }

    val dbRdd = sc.makeRDD(DBRows)

    val df = dbRdd.map {
      case Row(s0, s1, s2, s3, s4, s5, s6) => OldDBRecord(s0.asInstanceOf[Int], s1.asInstanceOf[String], s2.asInstanceOf[String], s3.asInstanceOf[String], s4.asInstanceOf[Double], s5.asInstanceOf[Double], s6.asInstanceOf[String])
    }.toDF()
    MongoSpark.save(df.write.option("collection", AppConf.firstPhaseCollection).mode("append"))
  }

  def buildCategoriesDB(DS: Dataset[Triple],path:String){
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
