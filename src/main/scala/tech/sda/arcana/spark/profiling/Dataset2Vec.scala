package tech.sda.arcana.spark.profiling

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset

object Dataset2Vec {
      val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Dataset2Vec")
      .getOrCreate()
  
  def fetchSubjectsRelatedToWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Subject from triples where Object like '%$word%'") //> RLIKE for regular expressions
      return Res
  }
  def fetchObjectssRelatedToWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Object from triples where Subject like '%$word%'") //> RLIKE for regular expressions
      return Res
  }
  def main(args: Array[String]) {
          
      val input="src/main/resources/rdf.nt"
      val R=RDFApp.exportingData(input)

      val Res=fetchSubjectsRelatedToWord(R.toDF(),"Netherlands")
      Res.show(false)
     
      //Breadth First Search 
      
    println("~Stopping Session~")
    spark.stop()
  }
}