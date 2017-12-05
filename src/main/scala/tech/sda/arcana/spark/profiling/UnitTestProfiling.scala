package tech.sda.arcana.spark.profiling
import org.apache.spark.sql.SparkSession
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

   def main(args: Array[String]) = {

    //val textFile = sc.textFile("/home/elievex/Repository/resources/"+AppConf.Questions)
    //val noEmptyRDD = textFile.filter(x => (x != null) && (x.length > 0))
    //noEmptyRDD.foreach(println)


val path="/home/elievex/Repository/resources/"
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
  
      val categories = AppConf.categories
      var DBRows = ArrayBuffer[Row]()
//DBRecord(uri: String, expression: String, category: String, senti_n: Double, senti_v: Double, senti_a: Double, senti_r: Double, senti_uclassify: Double, objectOf: String, cosine_similary: Double)  
      //categories.foreach(println)
      val RDFDs=RDFApp.importingData(path+AppConf.dbpedia) 
      val sentiDF = SentiWord.readProcessedSentiWord("/home/elievex/Repository/resources/")
      val modelvec=Word2VecModelMaker.loadWord2VecModel(path+AppConf.Word2VecModel)
      
      
      for (x <- categories) {
      val myUriList = Dataset2Vec.fetchAllOfWordAsSubject(RDFDs.toDF(), x)
      myUriList.foreach(x=>println(x.Uri))
      //| Get different POS scores for category x
        println(x)
        val resultneg="-9"
        val sentiPosScore= SentiWord.getSentiScoreForAllPOS(x,sentiDF)  // 4 vals
        if(sentiPosScore(0)=="-9" && sentiPosScore(1)=="-9" && sentiPosScore(2)=="-9" && sentiPosScore(3)=="-9")
        {
          val result = APIData.getRankUclassify(x)
          val resultneg = result._1
        }
      
        for (y <- myUriList) {
          DBRows += Row(y.Uri, AppDBM.getExpFromSubject(y.Uri), x,sentiPosScore(0),sentiPosScore(1),sentiPosScore(2),sentiPosScore(3), resultneg, "", "")
//DBRecord(uri: String, expression: String, category: String, senti_n: Double, senti_v: Double, senti_a: Double, senti_r: Double, senti_uclassify: Double, objectOf: String, cosine_similary: Double)  

          
          val synResult=""
          try {
             val synonyms = modelvec.findSynonyms(y.Uri, 1000)
             val synResult = synonyms.filter("similarity>=0.4").as[Synonym].collect
               for (synonym <- synResult) {
                      val resultneg="-9"
                      val synword = AppDBM.getExpFromSubject(synonym.word)
                      val sentiPosScore= SentiWord.getSentiScoreForAllPOS(synword,sentiDF)  // 4 vals
                      if(sentiPosScore(0)=="-9" && sentiPosScore(1)=="-9" && sentiPosScore(2)=="-9" && sentiPosScore(3)=="-9")
                      {
                        val result = APIData.getRankUclassify(synonym.word)
                        val resultneg = result._1
                      }
                  DBRows += Row(synonym.word, AppDBM.getExpFromSubject(synonym.word), x,sentiPosScore(0),sentiPosScore(1),sentiPosScore(2),sentiPosScore(3),resultneg,y.Uri, synonym.similarity)
                 }
            } catch {
                 case e: Exception => println("didn't find synonyms for: "+y.Uri)
            }
            
            //synonyms.show()
            
            synResult.foreach(println)
            println("II ll keep goin")

        }
      
      }
			
    

          
          /*
   var A = Array("One", "Two", "Three", "Four", "Five", "Six")
   var B = Array("Seven", "Eight", "Nine", "Ten", "Eleven", "Twelve")
          
   val buf = scala.collection.mutable.ArrayBuffer.empty[String]
   
   A.foreach{x=>
     buf+=x
     B.foreach{y=>
        buf+=y
     }
   }
          
   buf.foreach(println)

          */

    println("YA")
    spark.stop()
   }
}


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
