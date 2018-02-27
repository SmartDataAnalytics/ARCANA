package tech.sda.arcana.spark.profiling
 import java.io.File
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2VecModel
//import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/*
 * An Object that reads the prepared data and converts it to a Word2Vec Model 
 */
object Word2VecModelMaker {
      val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Word2VecModelMaker")
      .getOrCreate()
      
  // this is Spark's example
  def fetchCodedDataDF(): DataFrame={
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "B Bridget remembered the nightmthat her parents died in the fire her younger sister Brandy and her was at a sleepover at a church youth camp. She remembered that their youth pastor took her aside while one of the women sat with Brandy and was informed of their deaths. It was her call to tell Brandy about their parents. One of their Sunday school teachers took them to their home. It was burned down to the ground with only the fireplace till standing but in bad shape.".split(" "),
      "The fire had spread to the family's un attached tool shed but left the garden house and un attached garage. Bridget told the authorities about a will that was in a bank safety deposit box and another one with their lawyer named Mack something. The authorities knew who Mack was and got a copy of their will. In the will they had no relatives so and they requested that a friend of theirs and a church member would finish raising the girls. The family that they had choose left the country on a three year mission trip. The judge ordered the girls into faster care.".split(" "),
      "With Brandy being eight years old and in elementry school adjusted to the school and had many friends that would continue their schooling in the same grade Brandy would. It was a lot harder for Bridget. With her looks she had boys interested in her but was shunned by the girls. She wa surrounded by some of the girls in the shower room and they cut her hair. Then they posted bad things about her using poster paper. They took her picture and made posters that she was available for a good time and used the foster parents telephone number ".split(" "),
      "I wish Java Matrix could use case classes".split(" "),
      "Java Matrix is a new technology that is awesome".split(" "),
      "I am going to buy the Java Matrix".split(" "),
      "I love Java".split(" "),
      "I love Matrix".split(" "),
      "Java is sun".split(" "),
      "Matrix is sun".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    documentDF
  }
  // Reading data form File (glitchy)
  def fetchFileDataDF(fileName:String): DataFrame={
    val sc = spark.sparkContext
    import spark.implicits._
    val input = sc.textFile(fileName).map(line => line.split(" ").toSeq).map(Tuple1.apply).toDF("text")
    input
  }
  // Reading data form File using Scala (non-glitchy)
  def fetchFileDataDFSc(fileName:String): DataFrame={
    val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    val bufferedSource = scala.io.Source.fromFile(fileName)
    val lines = (for (line <- bufferedSource.getLines()) yield line.split(" ")).toSeq
    val myDF = lines.map(Tuple1.apply).toDF("text")
    bufferedSource.close
    myDF
  }

  // Fit the data
  def fitWord2VecModel(fileName:DataFrame):Word2VecModel={
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(AppConf.VectorSize)
      .setMinCount(AppConf.MinCount)
      
    word2Vec.fit(fileName)
  }
  
  // List of files in directory
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
    } else {
        List[File]()
    }
  }
  // Return the file with .txt extension
  def returnTxtFile(dirFiles: List[File]): String = {
      var myfile=""
      for(x<-dirFiles){
        if(FilenameUtils.getExtension(x.toString())=="txt")
          myfile=x.toString()
      }
    myfile
  }
  // Save the model
  def saveWord2VecModel(path:String,model:Word2VecModel){
      model.write.overwrite().save(path+AppConf.Word2VecModel)
  }
  // Load model
  def loadWord2VecModel(fileName:String):Word2VecModel={
    Word2VecModel.load(fileName)
  }
 import spark.implicits._
  // get Synonyms
    def getWord2VecSynonyms(Word2VecModel:Word2VecModel,word:String,NumSyn:Int):Set[Synonym]={
    var synSet : Set[Synonym] = Set()
    try{
      val synonyms = Word2VecModel.findSynonyms(word, NumSyn)
      val synResult = synonyms.filter("similarity>=0.4").as[Synonym].collect
      synResult.foreach{
        f=>synSet+=f
      }
     }
    catch{
      case e: Exception => //println(s"didn't find synonyms for: $word")
    }
    synSet
  }
  
  def MakeWord2VecModel(path:String,Word2VecDataType:Int){
    val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    //| Pick one of these
    //val word2VecInput=fetchCodedDataDF()
    //val word2VecInput=fetchFileDataDF("src/main/resources/textTest.txt")
   
    val newPath = if (Word2VecDataType == 1) path+AppConf.CategoryData else path+AppConf.DatasetData
    
    // Search the directory word2vec data and find the txt one
    val word2vecData=(returnTxtFile(getListOfFiles(newPath)))
    val word2VecInput=fetchFileDataDFSc(word2vecData)
 
    //| Make model and save it
    val model = fitWord2VecModel(word2VecInput)
    saveWord2VecModel(path,model)
    println("~Word2Vec model is created~")
    //| Or load model
    //> val model = Word2VecModel.load("Word2VecModel")
    /*
    val synonyms = model.findSynonyms("<http://commons.dbpedia.org/resource/User:warTR1A>",1000)
    synonyms.show(false)
    val result = model.transform(word2VecInput)
    result.select("result").take(3).foreach(println)
    */
  }

  def main(args: Array[String]) {
   
    //MakeWord2VecModel()

    println("Stopping Session")
    spark.stop()
  }
}

     /*
     
    val temp = rawDF.map( x => {Seq((x._2)).map(Tuple1.apply)})
    temp.toDF().show(false)
    //val textDF = temp.map(x => x.split(" ")).map(Tuple1.apply).toDF("text")
    //val textDF = temp.map(x => x.split(" "))
    //val finDF= textDF.map(x=>Seq(x)).map(Tuple1.apply).toDF("text")
    //finDF.show(false)
    */