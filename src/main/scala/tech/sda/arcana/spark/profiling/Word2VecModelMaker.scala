package tech.sda.arcana.spark.profiling
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row

object Word2VecModelMaker {
      val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Word2VecUpdatedExample")
      .getOrCreate()
  def main(args: Array[String]) {
    val sc = spark.sparkContext
    import spark.implicits._
   // val rawDF = spark.sparkContext
    //.wholeTextFiles("Word2VecData")
    
    val input = sc.textFile("src/main/resources/textTest.txt").map(line => line.split(" ").toSeq).toDF("text")
    input.toDF().show(false)
    //.map(Tuple1.apply)
   
    val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(input)
    val result = model.transform(input)
    
    val synonyms = model.findSynonyms("school",1000)
    synonyms.show(false)
    
     /*
     
    val temp = rawDF.map( x => {Seq((x._2)).map(Tuple1.apply)})
    temp.toDF().show(false)
    //val textDF = temp.map(x => x.split(" ")).map(Tuple1.apply).toDF("text")
    //val textDF = temp.map(x => x.split(" "))
    //val finDF= textDF.map(x=>Seq(x)).map(Tuple1.apply).toDF("text")
    //finDF.show(false)
    */
    
    /*
    val word2Vec = new Word2Vec()
    .setInputCol("text")
    .setOutputCol("result")
    .setVectorSize(3)
    .setMinCount(0)
    val model = word2Vec.fit(input.toDF("text"))
    val result = model.transform(input.toDF("text"))
    result.select("result").take(3).foreach(println)
    */
   
    
    println("Stopping Session")
    spark.stop()
  }
}