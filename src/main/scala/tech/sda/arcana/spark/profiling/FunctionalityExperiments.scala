package tech.sda.arcana.spark.profiling
import org.tartarus.snowball

import org.apache.spark.mllib.feature.Stemmer

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
  .createDataFrame(Seq(("testing", 1), ("plays", 2), ("runs", 3)))
  .toDF("word", "id")

val stemmed = new Stemmer()
  .setInputCol("word")
  .setOutputCol("stemmed")
  .setLanguage("English")
  .transform(data)

stemmed.show

      
  }
}