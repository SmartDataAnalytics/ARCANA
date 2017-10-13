package tech.sda.arcana.spark.profiling

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import java.io._



object Dataset2Vec {
      val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Dataset2Vec")
      .getOrCreate()
      val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
      import sqlContext.implicits._
  def fetchSubjectsRelatedToObjectWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Subject from triples where Object like '%$word%'") //> RLIKE for regular expressions
      return Res
  }
  def fetchObjectsRelatedToSubjectWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Object from triples where Subject like '%$word%'") 
      return Res
  }
  def fetchSubjectsRelatedToWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Subject from triples where Subject like '%$word%'") 
      return Res
  }
  def fetchObjectsRelatedToWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Object from triples where Object like '%$word%'") 
      return Res
  }
 def appendToRDD(data: String) {
     val sc = spark.sparkContext
     val rdd = sc.textFile("Word2VecData")  
     val extraRDD=sc.parallelize(Seq(data))
     val newRdd = rdd ++ extraRDD
     //newRdd.map(_.toString).toDF.show()
     newRdd.map(_.toString).toDF.coalesce(1).write.format("text").mode("append").save("Word2VecData")
     //newRdd.map(_.toString).toDF.coalesce(1).write.format("text").mode("overwrite").save("Word2VecData")
 }
 
  def main(args: Array[String]) {
             

      val input="src/main/resources/rdf.nt"
      val R=RDFApp.exportingData(input)

      val Res=fetchSubjectsRelatedToObjectWord(R.toDF(),"Netherlands")
      //Res.show(false)
     
      val sc = spark.sparkContext
      
      val headerRDD= sc.parallelize(Seq("<http://commons.dbpedia.org/resource/File:Hunebed_015.jpg> <http://commons.dbpedia.org/resource/File:Hunebed_013.jpg>"))
      
      //Replace BODY part with your DF
      val bodyRDD= sc.parallelize(Seq("BODY2"))
      
      val footerRDD = sc.parallelize(Seq("FOOTER"))
      val extraRDD=sc.parallelize(Seq("FOOTER"))
      //combine all rdds to final    
      val finalRDD = headerRDD ++ bodyRDD ++ footerRDD ++ extraRDD
      
      //finalRDD.foreach(line => println(line))
      
      //output to one file
      //finalRDD.coalesce(1, true).saveAsTextFile("testMie")
      //finalRDD.saveAsTextFile("out\\int\\tezt")
      
      finalRDD.map(_.toString).toDF.coalesce(1).write.format("text").mode("overwrite").save("Word2VecData")
      appendToRDD("""<http://commons.dbpedia.org/resource/File:Paddestoel_002.jpg>""")

      
      /*
     val rdd = sc.textFile("Word2VecData")
     rdd.map(_.toString).toDF.show()
     val rddnew = rdd ++ headerRDD
     rddnew.map(_.toString).toDF.show()
   
     rddnew.map(_.toString).toDF.coalesce(1).write.format("text").mode("append").save("Word2VecData")
     */
     
     //val finalRDDD=rddnew.map(_.toString).toDF
     
    // val bodyRDxD= sc.parallelize(Seq("BODYx"))
     
     //bodyRDxD.map(_.toString).toDF.coalesce(1).write.format("text").mode("append").save("Word2VecData") // 'overwrite', 'append', 'ignore', 'error'.
      //finalRDD.map(_.toString).toDF.write.mode("append").text("testMie")

      
      //Breadth First Search 
      
    println("~Stopping Session~")
    spark.stop()
  }
}