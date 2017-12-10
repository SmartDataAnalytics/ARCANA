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
      
      


    //val RDFDs=RDFApp.importingData("/home/elievex/Repository/resources/")
    //val myDF=RDFApp.readProcessedData("/home/elievex/Repository/resources/")
    
 
    val URIs = ProcessQuestion.fetchTokenUris("tr","/home/elievex/Repository/resources/")
   // URIs.foreach(println)
      
    val DF=AppDBM.readDBCollection(AppConf.firstPhaseCollection)
    DF.createOrReplaceTempView("DB")

    URIs.foreach{t=>
      val res2 = spark.sql(s"SELECT  * FROM DB where uri = '$t'  ")
      //val res = spark.sql(s"SELECT  category,max(cosine_similary),expression,senti_a,senti_n,senti_r,senti_v,uri FROM DB where uri = '$t' group by(uri) ")
      val res = spark.sql(s"SELECT * FROM DB WHERE (uri,cosine_similary) IN ( SELECT  uri,max(cosine_similary) FROM DB where uri = '$t' group by(uri))")
      res.show(false)
      println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
      res2.show(false)
    }
  
//(uri: String, expression: String, category: String, senti_n: Double, senti_v: Double, senti_a: Double, senti_r: Double, senti_uclassify: Double, relatedTo: String, cosine_similary: Double)
 
    
    /*
    val word = "nuclearbomb"
    val res = spark.sql(s"SELECT rsc FROM DB where word = '$word' ")
    res.show
    res.collect().foreach(println)
    
    */
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
