package tech.sda.arcana.spark.profiling
import org.tartarus.snowball

import org.apache.spark.stemming.feature.Stemmer

//import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.SparkContext._

object FunctionalityExperiments {
   val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Experiments")
      .getOrCreate()
  def main(args: Array[String]) = {
      val sc = spark.sparkContext
     
val data = spark
  .createDataFrame(Seq(("testing", 1), ("killing", 2), ("runs", 3)))
  .toDF("word", "id")

val stemmed = new Stemmer()
  .setInputCol("word")
  .setOutputCol("stemmed")
  .setLanguage("English")
  .transform(data)

stemmed.show
  import org.apache.spark.sql.functions._
  import com.databricks.spark.corenlp.functions._
  
  import spark.implicits._
  
  val input = Seq(
    (1, "<xml>Stanford University is located in California. It is a great university.</xml>")
  ).toDF("id", "text")
  
  val output = input
    .select(cleanxml('text).as('doc))
    .select(explode(ssplit('doc)).as('sen))
    .select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))
 
  output.show(truncate = false)
       
  }
}