package tech.sda.arcana.spark.profiling
import util.control.Breaks._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._
import com.mongodb.spark._
import com.mongodb.spark.config._
object UnitTestProfiling {

    val spark = SparkSession.builder
    .master("local[*]")
    .config(AppConf.inputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
    .config(AppConf.outputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
    .appName("Word2VecModelMaker")
    .getOrCreate()
     val sc = spark.sparkContext
  
  
    import spark.implicits._

   def main(args: Array[String]) = {


      /* experimenting phase 2 
   
    val input="How to kill a rabit then a person?"
    val question = input.toLowerCase()
    val questionInfo = ProcessQuestion.ProcessSentence(question,path)
    val questionObj = new QuestionObj(question,ProcessQuestion.removeStopWords(ProcessQuestion.stringToDF(question)),ProcessQuestion.sentiment(question),questionInfo._1,questionInfo._2,0)
    questionObj.phaseTwoScore=ProcessQuestion.expressionCheck(questionObj)
 
      println(questionObj)
      */
 
 
    //println(ProcessQuestion.extractMostExactSenti("<http://commons.dbpedia.org/resource/User:TR2F>", "VBZ",DF))
     /*
    val word1 = List(("<http://commons.dbpedia.org/resource/User:TR4A>",-99d),("<http://commons.dbpedia.org/resource/User:TR4A>",0.3d),("<http://commons.dbpedia.org/resource/User:TR4A>",0.5d),("<http://commons.dbpedia.org/resource/User:TR3D>",0.2d),("<http://commons.dbpedia.org/resource/User:NA451>",0.6d))
    val word2 = List(("<http://commons.dbpedia.org/resource/User:TR9A>",-99d),("<http://commons.dbpedia.org/resource/User:TR4A>",-99d),("<http://commons.dbpedia.org/resource/User:TR4A>",0.2d),("<http://commons.dbpedia.org/resource/User:TR3D>",0.6d),("<http://commons.dbpedia.org/resource/User:NA451>",0.3d))
   
    var counter = 0 
    var sum = 0.0
    if(word1.size>0){
    word1.foreach{f=>
      if(f._2!= -99){
        sum+=f._2
        counter+=1
      }
    }
    println(sum)
    println(sum/counter)
    println(word1.size)
    }*/
    
    //ProcessQuestion.getTokenUrisSentiScore(list,"VBZ",DF)


    /*
     * 
    var foundFlag=false
    DF.createOrReplaceTempView("DB")
    val testcase = "<http://commons.dbpediax.org/resource/User:TR2F>"
    val postestcase = "VBZ"
    var myVoteScore = new ListBuffer[(DBRecord,String)]()
    val dbPOS=stanfordPOS2sentiPos(postestcase)
    val sentiArray = Array("senti_n","senti_v","senti_r","senti_a","senti_uclassify")
    //| I did this because if I searched for the sentiment analysis of a POS and it wasn't found I will need to query the DB again to fetch an alternative 
    //| by doing this if I didn't find the score I want I can get it from the other without querying the data again 
    var sentiScore=new DBRecord("","","",0.0,0.0,0.0,0.0,0.0,"",0.0)
    var sentiIndicator=""
    val allDB = spark.sql(s"SELECT  * FROM DB where uri = '$testcase'  ") 
    if(allDB.count()>0){
      allDB.show()
      allDB.createOrReplaceTempView("QuestionURI")
      val specDB = spark.sql(s"SELECT * FROM DB WHERE (uri,$dbPOS) IN ( SELECT  uri,max($dbPOS) FROM QuestionURI where uri = '$testcase' group by(uri))")
      if(specDB.count()>0){
        //specDB.show()
        val result = specDB.as[DBRecord].collect()
        if(isScoreNine(result(0),dbPOS)){
            sentiArray.foreach{x=>
              if(x!=dbPOS){
                 val sentiDB = spark.sql(s"SELECT * FROM DB WHERE (uri,$x) IN ( SELECT  uri,max($x) FROM QuestionURI where uri = '$testcase' group by(uri))")
                 val sentiResult = sentiDB.as[DBRecord].collect()
                 if(!isScoreNine(sentiResult(0),x)){
                   myVoteScore+=((sentiResult(0),x))              
                 }
              }
            }
        }else{
          foundFlag=true
          sentiScore=result(0)
          sentiIndicator=dbPOS
        }
      }      
    }


    if(myVoteScore.size>0){
      var Temp=myVoteScore(0)
      myVoteScore.foreach{t=>
        if(mapIndicator(t._1,t._2)>=mapIndicator(Temp._1,Temp._2)){
          Temp=t
        }
      }
      println("SCORE")
      println(mapIndicator(Temp._1,Temp._2),Temp._2)
    }
    else{
      if(foundFlag){
          println("SCORE")
          println(mapIndicator(sentiScore,sentiIndicator),sentiIndicator)}
    }
    
    
    */
    
      //res.show(false)
      //println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
      //res2.show(false)
/*
             val senti_nDB = spark.sql(s"SELECT * FROM DB WHERE (uri,senti_n) IN ( SELECT  uri,max(senti_n) FROM QuestionURI where uri = '$testcase' group by(uri))")
            val senti_nResult = senti_nDB.as[DBRecord].collect()
            println(senti_nResult(0).senti_uclassify)
            if(isScoreNine(uclassifyResult(0),"senti_n")){
              val senti_rDB = spark.sql(s"SELECT * FROM DB WHERE (uri,senti_r) IN ( SELECT  uri,max(senti_r) FROM QuestionURI where uri = '$testcase' group by(uri))")
              val senti_rResult = senti_rDB.as[DBRecord].collect()
              println(senti_rResult(0).senti_uclassify)
              if(isScoreNine(uclassifyResult(0),"senti_r")){
                val senti_aDB = spark.sql(s"SELECT * FROM DB WHERE (uri,senti_a) IN ( SELECT  uri,max(senti_a) FROM QuestionURI where uri = '$testcase' group by(uri))")
                val senti_aResult = senti_aDB.as[DBRecord].collect()
                println(senti_aResult(0).senti_uclassify)   
                if(isScoreNine(uclassifyResult(0),"senti_a")){
                  
                }
              }
            }    
 */
    
    /*
    URIs.foreach{t=>
      val res2 = spark.sql(s"SELECT  * FROM DB where uri = '$t'  ")
      //val res = spark.sql(s"SELECT  category,max(cosine_similary),expression,senti_a,senti_n,senti_r,senti_v,uri FROM DB where uri = '$t' group by(uri) ")
      //val res = spark.sql(s"SELECT * FROM DB WHERE (uri,cosine_similary) IN ( SELECT  uri,max(cosine_similary) FROM DB where uri = '$t' group by(uri))")
      val res = spark.sql(s"SELECT * FROM DB WHERE (uri,senti_uclassify) IN ( SELECT  uri,max(senti_uclassify) FROM DB where uri = '$t' group by(uri))")
      res.show(false)
      //println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
      res2.show(false)
    }
  */
//(uri: String, expression: String, category: String, senti_n: Double, senti_v: Double, senti_a: Double, senti_r: Double, senti_uclassify: Double, relatedTo: String, cosine_similary: Double)
 
/*
    def extractMostExactSenti(uriString:String,posString:String,DF:DataFrame):Double={
    println("I AM DYING HERE0")
     DF.show()
     println("I AM DYING HERE")
     
    var resultList : List[(String,Double)] = List()
    var finalResult = -99d
    var foundFlag=false
    DF.createOrReplaceTempView("DB")
    var myVoteScore = new ListBuffer[(DBRecord,String)]()
    val dbPOS=stanfordPOS2sentiPos(posString)
    val sentiArray = Array("senti_n","senti_v","senti_r","senti_a","senti_uclassify")
    //| I did this because if I searched for the sentiment analysis of a POS and it wasn't found I will need to query the DB again to fetch an alternative 
    //| by doing this if I didn't find the score I want I can get it from the other without querying the data again 
    var sentiScore=new DBRecord("","","",0.0,0.0,0.0,0.0,0.0,"",0.0)
    var sentiIndicator=""
    val allDB = spark.sql(s"SELECT  * FROM DB where uri = '$uriString'  ") 
    if(allDB.count()>0){
      allDB.show(false)
      allDB.createOrReplaceTempView("QuestionURI")
      val specDB = spark.sql(s"SELECT * FROM DB WHERE (uri,$dbPOS) IN ( SELECT  uri,max($dbPOS) FROM QuestionURI where uri = '$uriString' group by(uri))")
      if(specDB.count()>0){
        //specDB.show()
        val result = specDB.as[DBRecord].collect()
        if(isScoreNine(result(0),dbPOS)){
            sentiArray.foreach{x=>
              if(x!=dbPOS){
                 val sentiDB = spark.sql(s"SELECT * FROM DB WHERE (uri,$x) IN ( SELECT  uri,max($x) FROM QuestionURI where uri = '$uriString' group by(uri))")
                 val sentiResult = sentiDB.as[DBRecord].collect()
                 if(!isScoreNine(sentiResult(0),x)){
                    myVoteScore+=((sentiResult(0),x)) 
                 }
              }
            }
        }else{
          foundFlag=true
          sentiScore=result(0)
          sentiIndicator=dbPOS
        }
      }      
    }
    if(myVoteScore.size>0){
      var Temp=myVoteScore(0)
      myVoteScore.foreach{t=>
        if(mapIndicator(t._1,t._2)>=mapIndicator(Temp._1,Temp._2)){
          Temp=t
        }
      }
      finalResult = mapIndicator(Temp._1,Temp._2)
    }
    else{
      if(foundFlag){
        finalResult = mapIndicator(sentiScore,sentiIndicator)
      }
    }
    finalResult
   }
  def getTokenUrisSentiScore(list:List[String],posString:String,DF:DataFrame):ListBuffer[(String,Double)]={
    
    var UrisScore = new ListBuffer[(String,Double)]()
    if(list.size>0){
      list.foreach{
        t=>
        UrisScore+=((t,extractMostExactSenti(t,posString,DF)))
      }
    }
    //UrisScore.foreach(println)
    UrisScore
  }
 */

    println("-------")
    spark.stop()
   }
}

//val t1 = System.nanoTime
//  Syntactic category: n for noun files, v for verb files, a for adjective files, r for adverb files.

// How to kill an animal
//he writes that and she is writing it now he was walking and she walks today there?
//Hi There How are you There was a car walking by a dog nearby the horse?

//POS TAGS
/*
CC Coordinating conjunction
CD Cardinal number
DT Determiner
EX Existential there
FW Foreign word
IN Preposition or subordinating conjunction
JJ Adjective
JJR Adjective, comparative
JJS Adjective, superlative
LS List item marker
MD Modal
NN Noun, singular or mass
NNS Noun, plural
NNP Proper noun, singular
NNPS Proper noun, plural
PDT Predeterminer
POS Possessive ending
PRP Personal pronoun
PRP$ Possessive pronoun
RB Adverb
RBR Adverb, comparative
RBS Adverb, superlative
RP Particle
SYM Symbol
TO to
UH Interjection
VB Verb, base form
VBD Verb, past tense
VBG Verb, gerund or present participle
VBN Verb, past participle
VBP Verb, non­3rd person singular present
VBZ Verb, 3rd person singular present
WDT Wh­determiner
WP Wh­pronoun
WP$ Possessive wh­pronoun
WRB Wh­adverb
 */

 /*
    the subject, which is an RDF URI reference or a blank node
    the predicate, which is an RDF URI reference
    the object, which is an RDF URI reference, a literal or a blank node
 */
// Problamatic cases
//<http://simple.dbpedia.org/resource/4'33%22> <http://www.w3.org/2000/01/rdf-schema#label> "4'33\""@en .
//<http://simple.dbpedia.org/resource/%5C> <http://www.w3.org/2000/01/rdf-schema#label> "\\"@en .
//<http://simple.dbpedia.org/resource/%22beat-em_up%22> <http://www.w3.org/2000/01/rdf-schema#label> "\"beat-em up\""@en .
//<http://simple.dbpedia.org/resource/%22Captain%22_Lou_Albano> <http://www.w3.org/2000/01/rdf-schema#label> "\"Captain\" Lou Albano"@en .
