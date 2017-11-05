package tech.sda.arcana.spark.profiling
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import java.io._
import org.apache.spark.rdd.RDD
/*
# The pair (POS,ID) uniquely identifies a WordNet (3.0) synset.
# The values PosScore and NegScore are the positivity and negativity
# score assigned by SentiWordNet to the synset.
# The objectivity score can be calculated as:
# ObjScore = 1 - (PosScore + NegScore)
# SynsetTerms column reports the terms, with sense number, belonging
# to the synset (separated by spaces).

# POS	ID	PosScore	NegScore	SynsetTerms	Gloss
*/

object SentiWord {
      val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SentiWord")
      .getOrCreate()
    import org.apache.spark.sql.Row
   
    def convertSentiWordIntoDF(filename:String):DataFrame={
      val sc = spark.sparkContext
      import spark.implicits._
      var DBRows = ArrayBuffer[Row]()
      val rawDF = spark.sparkContext.textFile(filename) 
      val DF = rawDF.map(x => x.split("\t")).map( x=> SentiWordNetClass(x(0),x(1),x(2),x(3),x(4)) ).toDF()

      val result = DF.as[SentiWordNetClass].collect()

      for(x<-result){
        //println(x.POS+" "+x.ID+" "+x.PosScore+" "+x.NegScore+" "+x.SynsetTerms+" ")
        val terms = x.SynsetTerms.split(" ")
        if(terms.size>1){
          for(q<-terms){
            val WordnRank=(q.split("#"))
            DBRows += Row(x.POS,x.ID,x.PosScore, x.NegScore, WordnRank(0),WordnRank(1))
          }
         }
        else{
            val WordnRank=(x.SynsetTerms.split("#"))
            DBRows += Row(x.POS,x.ID,x.PosScore, x.NegScore, WordnRank(0),WordnRank(1))
        }
      }
        val dbRdd = sc.makeRDD(DBRows)
        val df = dbRdd.map {
          case Row(s0, s1, s2, s3, s4, s5) => SentiWordSpark(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[String], s3.asInstanceOf[String], s4.asInstanceOf[String], s5.asInstanceOf[String])
        }.toDF()
       df 
    } 
 
    def main(args: Array[String]) = {
 
       val DF = convertSentiWordIntoDF("/home/elievex/Repository/ExtResources/SentiWordNet/home/swn/www/admin/dump/SentiWordNet.txt")
       DF.createOrReplaceTempView("senti")
       val Res = spark.sql(s"SELECT * from senti where Term = 'kill' ") 
       Res.show()

       //val rawDF = spark.sparkContext.textFile("/home/elievex/Repository/ExtResources/SentiWordNet/home/swn/www/admin/dump/SentiWordNet.txt") 
      
       
       
       //val DFN = rawDF.map(x => x.split("\t")).map( x=> SentiWordNetClass(x(0),x(1),x(2),x(3),x(4)) )
       //val DFNT=  DFN.map(MAPT)
       
      //val DFN = DF.as[SentiWord].map(mapperT)
      //val df2 = DF.map(x => (x.getString(1),x.getString(0).length))
      
      
      /*val DF =  rawDF.map(mapperT).toDS().cache()
      //DF.show()
      val dbRdd = sc.makeRDD(DBRows)

      val df = dbRdd.map {
        case Row(s0, s1, s2, s3, s4) => SentiWord(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[String], s3.asInstanceOf[String], s4.asInstanceOf[String])
      }.toDF()
      df.show()*/
 
    println("~Ending Session~")
    spark.stop()
    }
}