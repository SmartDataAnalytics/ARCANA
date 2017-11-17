package tech.sda.arcana.spark.profiling
import org.tartarus.snowball

import org.apache.spark.stemming.feature.Stemmer

//import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.SparkContext._
import edu.smu.tspell.wordnet._

object FunctionalityExperiments {
   val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Experiments")
      .getOrCreate()
  def main(args: Array[String]) = {
      val sc = spark.sparkContext
     /*
val data = spark
  .createDataFrame(Seq(("having writes", 1), ("killing", 2), ("runs", 3)))
  .toDF("word", "id")

val stemmed = new Stemmer()
  .setInputCol("word")
  .setOutputCol("stemmed")
  .setLanguage("English")
  .transform(data)

 stemmed.show
  */
   
     val myStringArray: Array[String] = Array("kill")
     if (myStringArray.length > 0) {
     val buffer: StringBuffer = new StringBuffer()
        for (i <- 0 until myStringArray.length) {
          buffer.append((if (i > 0) " " else "") + myStringArray(i))
        }
     val wordForm: String = buffer.toString
     System.setProperty("wordnet.database.dir", "src/WordNet/3.0/dict")
     val database: WordNetDatabase = WordNetDatabase.getFileInstance
     val synsets: Array[Synset] = database.getSynsets(wordForm)
     
      if (synsets.length > 0) {
      println("The following synsets contain '" + wordForm + "' or a possible base form " +
          "of that text:")
      for (i <- 0 until synsets.length) {
        println("")
        val wordForms: Array[String] = synsets(i).getWordForms
        for (j <- 0 until wordForms.length) {
          System.out.print((if (j > 0) ", " else "") + wordForms(j))
        }
        println(": " + synsets(i).getDefinition)
      }
      }else {
      System.err.println(
        "No synsets exist that contain " + "the word form '" +
          wordForm +
          "'")
    }
       }else {
      System.err.println(
        "You must specify " + "a word form for which to retrieve synsets.")
    }
     
     spark.stop()
  }
}















