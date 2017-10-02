package tech.sda.arcana.spark.representation
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
object Word2VecSpark {
/*
   def main(args: Array[String]): Unit = {
     println("HI")
    val conf = new SparkConf().setAppName("Word2VecExample").setMaster("spark")
    val sc = new SparkContext(conf)

    // $example on$
    val input = sc.textFile("src/main/resources/text8_10000").map(line => line.split(" ").toSeq)
   }
    */
  

def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Word2VecExample")
      .getOrCreate()
        
    val sc = sparkSession.sparkContext    
    val input = sc.textFile("src/main/resources/text8_10000").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("a", 5)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    //| Save and load model
      model.save(sc, "src/main/resources/Model_2")
    //> val sameModel = Word2VecModel.load(sc, "myModelPath")
 
    //| Read parquet file into DataFrame
    //> val df = sparkSession.read.parquet("src/main/resources/Model_2/data/part-r-00000-27a4dab4-4fe8-4e14-b44d-95866939ddf4.snappy.parquet")
    //> df.show()
    
    sparkSession.stop()
    println("FINISH PROCESSING")
  }
}