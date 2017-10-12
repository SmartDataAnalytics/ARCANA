package tech.sda.arcana.spark.profiling

import org.apache.spark.sql.SparkSession

object Dataset2Vec {
  
  def getUriRelatedToWord(x: String): String = {
    "Uri"
  }
  
  def getInfoOfUri(x: String): String = {
    "Info"
  }
  
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Dataset2Vec")
      .getOrCreate()
      
      println("Dataset2Vec")

      val input="src/main/resources/rdf.nt"
      val R=RDFApp.exportingData(input)
      R.show()
    //triples.createOrReplaceTempView("triples2")
    

    //val teenagersDF = spark.sql("SELECT * from triples2 where Subject like '%Hunebed%'") //> RLIKE for regular expressions
    //teenagersDF.show(false)

    println("~Stopping Session~")
    spark.stop()
  }
}