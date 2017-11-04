package tech.sda.arcana.spark.profiling

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
case class SentiWord(POS:String, ID:String, PosScore:String, NegScore:String, SynsetTerms:String)
    
object SentiWord {
      val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SentiWord")
      .getOrCreate()
    import org.apache.spark.sql.Row

    var DBRows = ArrayBuffer[Row]()
    def mapperT(line:String):SentiWord={
       val fields= line.split("\t")
       DBRows += Row(fields(0),fields(1),fields(2),fields(3),fields(4))
       //println(fields(0)+"-"+fields(1)+"-"+fields(2)+"-"+fields(3)+"-"+fields(4)+"-"+fields(5))
       SentiWord(fields(0),fields(1),fields(2),fields(3),fields(4))
    } 

    def main(args: Array[String]) = {
      val sc = spark.sparkContext
      //val file = sc.textFile("/home/elievex/Repository/ExtResources/SentiWordNet/home/swn/www/admin/dump/SentiWordNet.txt").map(line => line.split("\t").map(_.trim))
      //file..show()
      
      import spark.implicits._
      val rawDF = spark.sparkContext.textFile("/home/elievex/Repository/ExtResources/SentiWordNet/home/swn/www/admin/dump/SentiWordNet.txt") 
      val DF =  rawDF.map(mapperT).toDS().cache()
      //DF.show()
      val dbRdd = sc.makeRDD(DBRows)

      val df = dbRdd.map {
        case Row(s0, s1, s2, s3, s4) => SentiWord(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[String], s3.asInstanceOf[String], s4.asInstanceOf[String])
      }.toDF()
      df.show()
      /*
      val DF = rawDF.map(x => x.split("\t")).map( x=> SentiWord(x(0),x(1),x(2),x(3),x(4)) ).toDF()
      val DFN = DF.as[SentiWord].map(mapperT)
      val df2 = DF.map(x => (x.getString(1),x.getString(0).length))
		  */
    println("~Ending Session~")
    spark.stop()
    }
}