package tech.sda.arcana.spark.profiling
import org.apache.spark.sql.SparkSession
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
   def stanfordPOS2sentiPos(postestcase:String):String={
      var dbPOS=""
      //a for adjective files
      //r for adverb files
      if(postestcase=="NN"||postestcase=="NNS"||postestcase=="NNP"||postestcase=="NNPS"){
        dbPOS="senti_n"
      }else if(postestcase=="VBZ"||postestcase=="VB"||postestcase=="VBD"||postestcase=="VBG"||postestcase=="VBN"||postestcase=="VBP"){
        dbPOS="senti_v"
      }else if(postestcase=="RB"||postestcase=="RBR"||postestcase=="RBS"){
        dbPOS="senti_r"
      }else if(postestcase=="JJ"||postestcase=="JJR"||postestcase=="JJS"){
        dbPOS="senti_a"
      }
      dbPOS
    }
   def isScoreNine(obj:DBRecord,pos:String):Boolean={
     var flag = false
     pos match {
      case "senti_v"  => if(obj.senti_v.toDouble==(-9.0)){flag = true}
      case "senti_n"  => if(obj.senti_n.toDouble==(-9.0)){flag = true}
      case "senti_r"  => if(obj.senti_r.toDouble==(-9.0)){flag = true}
      case "senti_a"  => if(obj.senti_a.toDouble==(-9.0)){flag = true}
      case "senti_uclassify"  => if(obj.senti_uclassify.toDouble==(-9.0)){flag = true}
      }
     flag
   }
   def main(args: Array[String]) = {


      /* experimenting phase 2 
    val path= "/home/elievex/Repository/resources/"
    val input="How to kill a rabit then a person?"
    val question = input.toLowerCase()
    val questionInfo = ProcessQuestion.ProcessSentence(question,path)
    val questionObj = new QuestionObj(question,ProcessQuestion.removeStopWords(ProcessQuestion.stringToDF(question)),ProcessQuestion.sentiment(question),questionInfo._1,questionInfo._2,0)
    questionObj.phaseTwoScore=ProcessQuestion.expressionCheck(questionObj)
 
      println(questionObj)
      */


    val URIs = ProcessQuestion.fetchTokenUris("tr","/home/elievex/Repository/resources/")
   // URIs.foreach(println)
      
    val DF=AppDBM.readDBCollection(AppConf.firstPhaseCollection)
    DF.createOrReplaceTempView("DB")
    val testcase = "<http://commons.dbpedia.org/resource/User:TR2F>"
    val postestcase = "VBZ"
    
    val dbPOS=stanfordPOS2sentiPos(postestcase)
    val sentiArray = Array("senti_n","senti_v","senti_r","senti_a")
    //| I did this because if I searched for the sentiment analysis of a POS and it wasn't found I will need to query the DB again to fetch an alternative 
    //| by doing this if I didn't find the score I want I can get it from the other without querying the data again 
    val allDB = spark.sql(s"SELECT  * FROM DB where uri = '$testcase'  ") 
    if(allDB.count()>0){
      //allDB.show()
      allDB.createOrReplaceTempView("QuestionURI")
      val specDB = spark.sql(s"SELECT * FROM DB WHERE (uri,$dbPOS) IN ( SELECT  uri,max($dbPOS) FROM QuestionURI where uri = '$testcase' group by(uri))")
      if(specDB.count()>0){
        specDB.show()
        val result = specDB.as[DBRecord].collect()
        if(isScoreNine(result(0),dbPOS)){
          val uclassifyDB = spark.sql(s"SELECT * FROM DB WHERE (uri,senti_uclassify) IN ( SELECT  uri,max(senti_uclassify) FROM QuestionURI where uri = '$testcase' group by(uri))")
          val uclassifyResult = uclassifyDB.as[DBRecord].collect()
          println(uclassifyResult(0).senti_uclassify)
          if(isScoreNine(uclassifyResult(0),"senti_uclassify")){
            sentiArray.foreach{x=>
              if(x!=dbPOS){
                 val sentiDB = spark.sql(s"SELECT * FROM DB WHERE (uri,$x) IN ( SELECT  uri,max($x) FROM QuestionURI where uri = '$testcase' group by(uri))")
                 val sentiResult = sentiDB.as[DBRecord].collect()
              }
            }

          }
         
        }
      }      
    }
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
 


    println("YA")
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
