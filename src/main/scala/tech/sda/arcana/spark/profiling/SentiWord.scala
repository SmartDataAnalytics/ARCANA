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
 * An Object that deals with SentiWordNet and makes its data queryable (Providing Score)
 */

/*
# The pair (POS,ID) uniquely identifies a WordNet (3.0) synset.
# The values PosScore and NegScore are the positivity and negativity
# score assigned by SentiWordNet to the synset.
# The objectivity score can be calculated as:
# ObjScore = 1 - (PosScore + NegScore)
# SynsetTerms column reports the terms, with sense number, belonging
# to the synset (separated by spaces).

# POS	ID	PosScore	NegScore	SynsetTerms	Gloss
# count	synsetId	synsetPOS	swnPositivity	swnNegativity	feedbackPositivity	feedbackNegativity	date	IPanon	IPcountry	list(word#sense)
*/

object SentiWord {
      val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("SentiWord")
      .getOrCreate()
      val sc = spark.sparkContext
      import spark.implicits._
    import org.apache.spark.sql.Row
    import spark.implicits._
    
    // Convert the SentiWordFeedback file to a form that is easy to deal with and query 
    def convertSentiWordFeedbackIntoDF(filename:String):DataFrame={
      var DBRows = ArrayBuffer[Row]()
      val rawDF = spark.sparkContext.textFile(filename) 
      val DF = rawDF.map(x => x.split("\t")).map( x=> SentiWordNetFeedbackClass(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10)) ).toDF()

      val result = DF.as[SentiWordNetFeedbackClass].collect()
      for(x<-result){
        //	list:String
        val terms = x.list.split(" ")
        if(terms.size>1){
          for(q<-terms){
            val WordnRank=(q.split("#"))
            DBRows += Row(x.synsetPOS,x.swnPositivity,x.swnNegativity, x.feedbackPositivity,x.feedbackNegativity, WordnRank(0),WordnRank(1))
          }
         }
        else{
            val WordnRank=(x.list.split("#"))
            DBRows += Row(x.synsetPOS,x.swnPositivity,x.swnNegativity, x.feedbackPositivity,x.feedbackNegativity, WordnRank(0),WordnRank(1))
        }
      }
        val dbRdd = sc.makeRDD(DBRows)
        val df = dbRdd.map {
          case Row(s0, s1, s2, s3, s4, s5, s6) => SentiWordNetFeedbackSpark(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[String], s3.asInstanceOf[String], s4.asInstanceOf[String], s5.asInstanceOf[String],s6.asInstanceOf[String])
        }.toDF()
       df 
    }
    // Convert the SentiWord file to a form that is easy to deal with and query 
    def convertSentiWordIntoDF(filename:String):DataFrame={
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

    def prepareSentiFile(fileName:String):DataFrame={      
      val DF = convertSentiWordIntoDF(fileName)
      DF
    }
    
    // Will return all the scores of a word when it takes place in different POS 
    def getSentiScoreForAllPOS(sword:String,DF:DataFrame):Array[String]={   
      val word=sword.toLowerCase
      DF.createOrReplaceTempView("senti")
      val Res = spark.sql(s"SELECT * from senti where Term = '$word'  ") // and POS='n' 
      Res.createOrReplaceTempView("Pos")
      val Pos = spark.sql(s"SELECT POS from senti where Term = '$word' group by POS  ")
      val POSList=Pos.select("POS").rdd.map(r => r(0)).collect()
      var a : List[(String,String)] = List()

      // loop over the POS related to the word 
       for(x <-POSList){
         val posCon=spark.sql(s"SELECT * from Pos where POS = '$x'")
         val resul = posCon.as[SentiWordSpark].collect()
         var Score=0.0
         var Sum=0.0
          for( x <- resul){
             Score += (x.PosScore.toDouble-x.NegScore.toDouble)/(x.TermRank.toDouble) // decide whether to keep the positive part or delete it 
             Sum+=(1/(x.TermRank.toDouble))
           }
         Score /= Sum
         //println(x+" --> SCORE IS"+Score)
         a = a:+((x.toString(),Score.toString()))
       }
        //  Syntactic category: n for noun files, v for verb files, a for adjective files, r for adverb files.   
        var SentiScore =  Array("-9", "-9","-9","-9","-9" )
        a.foreach{tuple => 
            tuple._1 match {
            case "n" => SentiScore(0)=tuple._2
            case "v" => SentiScore(1)=tuple._2
            case "a" => SentiScore(2)=tuple._2
            case "r" => SentiScore(3)=tuple._2
            // catch the default with a variable so you can print it
            case whoa => println("Unexpected case in SentiWord: " + whoa.toString)
            }
        }
      SentiScore
      }
    def writeProcessedSentiWord(path:String){
      val sentiDF=SentiWord.prepareSentiFile(path+AppConf.SentiWordFile)
      val sc = spark.sqlContext
      sentiDF.write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(path+AppConf.ProcessedSentiWordFile)
      println("~Processed SentiWord files are created~")
    }
    def readProcessedSentiWord(path:String):DataFrame={
      val sc = spark.sqlContext
      sc.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(path+AppConf.ProcessedSentiWordFile)
    }
    def main(args: Array[String]) = {
      val path = "/home/elievex/Repository/resources/"
      val sentiDF = SentiWord.readProcessedSentiWord(path)
      val sentiPosScore= SentiWord.getSentiScoreForAllPOS("good",sentiDF)
      println(sentiPosScore(0),sentiPosScore(1),sentiPosScore(2),sentiPosScore(3))
      //FEEDBACK sentiword
      /*
      val df=convertSentiWordFeedbackIntoDF(AppConf.SentiWordFilefeedback)
      df.show()
      println(df.count())
      */

      //score += setScore.getValue() / (double) setScore.getKey();
			//sum += 1.0 / (double) setScore.getKey();
			// Score= 1/2*first + 1/3*second + 1/4*third ..... etc.
			// Sum = 1/1 + 1/2 + 1/3 ...
  
    println("~Ending Session~")
    spark.stop()
    }
}