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
   // get Sentiment analysis for the word as N, V, R, A and if non was fetched then grab the Uclassify score too
   def getSentiScores(word:String,sentiDF:DataFrame):Array[String]={
        var resultneg="-9"
        val sentiPosScore= SentiWord.getSentiScoreForAllPOS(word,sentiDF)  // 5 vals
        if(sentiPosScore(0)=="-9" && sentiPosScore(1)=="-9" && sentiPosScore(2)=="-9" && sentiPosScore(3)=="-9")
        {

          val result = APIData.getRankUclassify(word)
          resultneg = result._1

        }
  
        sentiPosScore(4) = resultneg
        sentiPosScore
   }
   def main(args: Array[String]) = {

    //val textFile = sc.textFile("/home/elievex/Repository/resources/"+AppConf.Questions)
    //val noEmptyRDD = textFile.filter(x => (x != null) && (x.length > 0))
    //noEmptyRDD.foreach(println)
     val path="/home/elievex/Repository/resources/"
      val RDFDs = RDFApp.importingData(path+AppConf.dbpedia) 
      val sentiDF = SentiWord.readProcessedSentiWord("/home/elievex/Repository/resources/")
      val modelvec = Word2VecModelMaker.loadWord2VecModel(path+AppConf.Word2VecModel)
 
      
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
  
      val categories = AppConf.categories
      var DBRows = ArrayBuffer[Row]()
            val t3 = System.nanoTime
      for (x <- categories) {
        val myUriList = Dataset2Vec.fetchAllOfWordAsSubject(RDFDs.toDF(), x)

        val sentiPosScore = getSentiScores(x.toLowerCase,sentiDF) 
         
        for (y <- myUriList) {
          DBRows += Row(y.Uri, AppDBM.getExpFromSubject(y.Uri), x,sentiPosScore(0),sentiPosScore(1),sentiPosScore(2),sentiPosScore(3), sentiPosScore(4), "", "")
          //println(y.Uri)
          try {
             val synonyms = modelvec.findSynonyms(y.Uri, 1000)
             val synResult = synonyms.filter("similarity>=0.3").as[Synonym].collect

               for (synonym <- synResult) {
                  val synSentiPosScore = getSentiScores(AppDBM.getExpFromSubject(synonym.word),sentiDF)

                  DBRows += Row(synonym.word, AppDBM.getExpFromSubject(synonym.word), x,synSentiPosScore(0),synSentiPosScore(1),synSentiPosScore(2),synSentiPosScore(3),synSentiPosScore(4),y.Uri, synonym.similarity)
                 }
            } catch {
                 case e: Exception => println("didn't find synonyms for: "+y.Uri)
            }

        val dbRdd = sc.makeRDD(DBRows)
        val df = dbRdd.map {
          case Row(s0, s1, s2, s3, s4, s5, s6,s7,s8,s9) => DBRecord(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[String], s3.asInstanceOf[Double], s4.asInstanceOf[Double], s5.asInstanceOf[Double], s6.asInstanceOf[Double],s7.asInstanceOf[Double],s8.asInstanceOf[String],s9.asInstanceOf[Double])
        }.toDF()
        //MongoSpark.save(df.write.option("collection", AppConf.firstPhaseCollection).mode("append"))
        }
      }
     val t4 = System.nanoTime
      val t1 = System.nanoTime
      categories.foreach{x=>
       val myUriList = Dataset2Vec.fetchAllOfWordAsSubject(RDFDs.toDF(), x)
       val sentiPosScore = getSentiScores(x.toLowerCase,sentiDF) 
       myUriList.foreach{y=>
         DBRows += Row(y.Uri, AppDBM.getExpFromSubject(y.Uri), x,sentiPosScore(0),sentiPosScore(1),sentiPosScore(2),sentiPosScore(3), sentiPosScore(4), "", "")
         
          try{
            val synonyms = modelvec.findSynonyms(y.Uri, 1000)
            val synResult = synonyms.filter("similarity>=0.3").as[Synonym].collect
            synResult.foreach{synonym=>
              val synSentiPosScore = getSentiScores(AppDBM.getExpFromSubject(synonym.word),sentiDF)
              DBRows += Row(synonym.word, AppDBM.getExpFromSubject(synonym.word), x,synSentiPosScore(0),synSentiPosScore(1),synSentiPosScore(2),synSentiPosScore(3),synSentiPosScore(4),y.Uri, synonym.similarity)
            }
          } 
          catch{
            case e: Exception => println("didn't find synonyms for: "+y.Uri)
          }
          
          val dbRdd = sc.makeRDD(DBRows)
          val df = dbRdd.map {
          case Row(s0, s1, s2, s3, s4, s5, s6,s7,s8,s9) => DBRecord(s0.asInstanceOf[String], s1.asInstanceOf[String], s2.asInstanceOf[String], s3.asInstanceOf[Double], s4.asInstanceOf[Double], s5.asInstanceOf[Double], s6.asInstanceOf[Double],s7.asInstanceOf[Double],s8.asInstanceOf[String],s9.asInstanceOf[Double])
          }.toDF()
         }
       }
     val t2 = System.nanoTime
     
     //DBRows.foreach(println)
      
      
      
      

    println("Elapsed time: "+(t2-t1)+"ns")
    println("Elapsed time: "+(t4-t3)+"ns")
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
