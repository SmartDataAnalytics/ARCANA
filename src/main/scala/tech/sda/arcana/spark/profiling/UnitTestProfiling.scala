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
import org.apache.commons.lang.StringEscapeUtils
import java.io.File
import java.nio.charset.Charset
import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.functions._
import scala.io.Source
import com.google.common.io.Files
import edu.stanford.nlp.process.CoreLabelTokenFactory
import edu.stanford.nlp.ling.CoreAnnotations.{PartOfSpeechAnnotation, SentencesAnnotation, TextAnnotation, TokensAnnotation}
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TextAnnotation, TokensAnnotation}
import util.control.Breaks._
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentClass
import edu.stanford.nlp.coref.CorefCoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2VecModel
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2VecModel
object UnitTestProfiling {

    val spark = SparkSession.builder
    .master("local[*]")
    .config(AppConf.inputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
    .config(AppConf.outputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
    .appName("Word2VecModelMaker")
    .getOrCreate()
     val sc = spark.sparkContext
  
       
    import spark.implicits._

    val props: Properties = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
    
    def questionProcess(Line:String):List[String]={
      //DF.show()
      var dm  = List[RDFURI]()
      val Dbpedia = RDFApp.readProcessedData("/home/elievex/Repository/resources/"+AppConf.processedDBpedia)
      //println(Line)
      val text= Line
      val path = "/home/elievex/Repository/resources/"
      // create blank annotator
      val document: Annotation = new Annotation(text)   
      // run all Annotator - Tokenizer on this text
      pipeline.annotate(document)
  
      val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList
      val listOfLines = Source.fromFile(path+AppConf.StopWords).getLines.toList
        
      val Tokens=  (for {
          sentence: CoreMap <- sentences
          token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
          word: String = token.get(classOf[TextAnnotation])
          pos: String = token.get(classOf[PartOfSpeechAnnotation])
          lemma: String = token.get(classOf[LemmaAnnotation])
    
        } yield ( word, lemma))
        Tokens.foreach{t =>
          if(!listOfLines.contains(t._2))
          {
          //println(t)
          dm = Dataset2Vec.fetchAllOfWordAsSubject(Dbpedia,t._2)
          }
        }
      var DMC = new ListBuffer[String]()

      dm.foreach{
        
        x=>
          //println(x.Uri)
          DMC+=x.Uri
      }
      DMC.toList
    }
    
   def main(args: Array[String]) = {
    val path = "/home/elievex/Repository/resources/"
    val sc = spark.sparkContext
    val textFile = sc.textFile("/home/elievex/Repository/resources/questionfake/*")
    val noEmptyRDD = textFile.filter(x => (x != null) && (x.length > 0))
    //noEmptyRDD.foreach(println)
    val Dbpedia = RDFApp.readProcessedData("/home/elievex/Repository/resources/"+AppConf.processedDBpedia).cache().toDF()
    val ds=noEmptyRDD.map(x=>questionProcess(x))
    println("YES_1")
    ds.collect()
    /*
    ds.foreach{
      x => x.foreach{
        y => println(y)
      }
    }
    */
    //var fruits = new ListBuffer[String]()
    var TEXT : Set[String] = Set()
    //var synSet : Set[String] = Set()
    println("YES_2")

    val dr= ds.flatMap(x => x)
    val dt = dr.distinct()
    
    dt.saveAsTextFile("TESTME")

    println("YES_3")
    
    //TEXT.foreach(println)      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      
      /*
      val nounsLiteral = List("barack")
      
      var question ="dang barack obama"
      //var dm  = List[String]()
      
      val DF = AppDBM.readDBCollection(AppConf.secondPhaseCollection)
      DF.createOrReplaceTempView("DB")
      var flag=0
      var word = "dang"
        val res = spark.sql(s"SELECT distinct relationshipID FROM DB where verb = '$word' ")
        val rellist = res.select("relationshipID").rdd.map(r => r(0).asInstanceOf[Int]).collect()
        if(!rellist.isEmpty){
          println("ONE")
          flag=1
          //t.relationID=rellist(0)
          var relID=rellist(0)
          val resNoun = spark.sql(s"SELECT distinct noun FROM DB where relationshipID = '$relID' ")
          val UriList=resNoun.select("noun").rdd.map(r => r(0).asInstanceOf[String]).collect()
          
          nounsLiteral.foreach{
            f => if(UriList.contains(f)){
              println("FOUND IT: "+word+" "+f)
              flag=2
              break
            }
          }
          
        if(flag==1 && flag!=2){
        UriList.foreach{
          f => 
            if(question contains f){
            flag = 3
            println("FOUND EM OB")
            break
          }
        }
        
        
          }
          
        }
      

      */
      
     /*
      var tdf= RDFApp.readProcessedData("/home/elievex/Repository/resources/DBpedia/processedDataprob")
      tdf.createOrReplaceTempView("triples")
      val REG = raw"(XMLSchema#float)".r
      val Res = spark.sql(s"""SELECT Object from triples where Object NOT RLIKE "$REG"""")
      val UriList=Res.select("Object").rdd.map(r => r(0)).collect()
     // UriList.foreach(println)
      println(UriList(1))
      val word = UriList(1)
      val Res2 = spark.sql(s"""SELECT Object from triples where Subject = ' $word ' """) 
      */
      /*
      var tdf= RDFApp.readProcessedData("/home/elievex/Repository/resources/DBpedia/processedData")
      
      val word="terrorism"
      tdf.createOrReplaceTempView("triples")
      //println("Word is: "+word)
      val REG = raw"(?i)(?<![a-zA-Z])$word(?![a-zA-Z])".r
      val Res = spark.sql(s"""SELECT distinct * from triples where Subject RLIKE "$REG" LIMIT 5 """)
      //val Res = spark.sql(s"SELECT * from triples where Subject like '%$word%'") 
      val UriList=Res.select("Subject").rdd.map(r => r(0)).collect()
      UriList:+APIData.fetchDbpediaSpotlight(word)
      //UriList.foreach(println)
      var XCV=UriList.toList.distinct.map(x => new RDFURI(x.asInstanceOf[String]))
      */
      
      /*
      val T1=Dataset2Vec.fetchAllOfWordAsSubject(RDFApp.readProcessedData("/home/elievex/Repository/resources/"+AppConf.processedDatafake),"kill")
      T1.foreach(println)
      var s : Set[String] = Set()
      T1.foreach(f=>s+=f.Uri)
      
       * //println(APIData.fetchDbpediaSpotlight("terrorism"))
       */
      //XCV.foreach(f=>println(f.Uri))
      //val list = Dataset2Vec.fetchAllOfWordAsSubject(tdf.toDF(),"terrorism")
      //list+=APIData.fetchDbpediaSpotlight("terrorism")
      //
    //  println(APIData.fetchDbpediaSpotlight("terrorism"))
     
      //curl "http://api.dbpedia-spotlight.org/en/annotate?text=War.&confidence=0.2&support=20" -H "Accept:application/json" 

    //val query = s""""http://api.dbpedia-spotlight.org/en/annotate?text=War.&confidence=0.2&support=20" -H "Accept:application/json""""
    //println(query)
    //def get(url: String) = scala.io.Source.fromURL(query).mkString
    //println(get(query))
    //println("ye")
    
    //val result3 = fetch(query)
    //val result3 = get(query)
      //println(result3)
        
        
      //var tdf= RDFApp.readProcessedData("/home/elievex/Repository/resources/DBpedia/processedDataprob")
      //val DFN=tdf.filter("Object not like '-'").toDF()
      //DFN.show()
      /*
      
      val REG = raw"(?<!\^)<.+>".r
      val String = """"^<http://www.w3.org/2001/XMLSchema#float>""""
      val String2 = """<http://www.w3.org/2001/XMLSchema#float>"""
      if(REG.findFirstIn(String)!=None){
        println("NO HAT")
      }else{
        println("HAT DETECTED")
      }*/
      //val REG = raw"(http://)".r
   //   val Res1 = spark.sql(s"""SELECT * from triples where Subject RLIKE "$REG" """)
      //tdf.createOrReplaceTempView("triples")
      //val word = "<http://commons.dbpedia.org/resource/User:nuclearA1>"
      //val Res2 = spark.sql(s"""SELECT Object from triples where Subject = "$word" and Object RLIKE  "$REG" """)
      
      //val query =  """SELECT Object from triples where Subject = ""55.92722222222222"^^<http://www.w3.org/2001/XMLSchema#float>" and Object RLIKE  "(http://)" """
      //val REG = raw"(http://)".r
      //val Res2 = spark.sql(s"""SELECT * from triples where Object RLIKE  "$REG" """)
      //val word ="SELECT Object from triples where Subject = ""55.92722222222222"^^<http://www.w3.org/2001/XMLSchema#float>" and Object RLIKE  "(http://)""
      //val Res = spark.sql(s"""SELECT Object from triples where Subject = "$word"""")
      //Res.show(false)
      
      // SELECT Object from triples where Subject = "$word" and Object RLIKE  "$REG" 
     /*
      val path = "/home/elievex/Repository/resources/"
      println("TEST")
      //val tdf =RDFApp.readProcessedData(path)
      

      
      
      
      
      val word ="<http://dbpedia.org/resource/Eighty_Years'_War>"
      val word2= StringEscapeUtils.escapeSql(word);
      println(s"""$word2""")
      //println(s"SELECT Object from triples where Subject = '$word'")
      tdf.createOrReplaceTempView("triples")
      //println(s"SELECT Object from triples where Subject = '$word'")
      val Res = spark.sql(s"""SELECT Object from triples where Subject = "$word2" """) 
      val UriList=Res.select("Object").rdd.map(r => r(0)).collect()
      UriList.foreach(println)
      //UriList.toList.distinct.map(x => new RDFURI(x.asInstanceOf[String]))
      
      */
   

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
   }term
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



/*
 * Exception in thread "main" org.apache.spark.sql.catalyst.parser.ParseException: 
extraneous input '_War' expecting {<EOF>, '.', '[', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'OR', 'AND', 'IN', NOT, 'BETWEEN', 'LIKE', RLIKE, 'IS', 'WINDOW', 'UNION', 'EXCEPT', 'INTERSECT', EQ, '<=>', '<>', '!=', '<', LTE, '>', GTE, '+', '-', '*', '/', '%', 'DIV', '&', '|', '^', 'SORT', 'CLUSTER', 'DISTRIBUTE', STRING}(line 1, pos 86)

== SQL ==
SELECT Object from triples where Subject = '<http://dbpedia.org/resource/Eighty_Years'_War>'
--------------------------------------------------------------------------------------^^^

	at org.apache.spark.sql.catalyst.parser.ParseException.withCommand(ParseDriver.scala:197)
	at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parse(ParseDriver.scala:99)
	at org.apache.spark.sql.execution.SparkSqlParser.parse(SparkSqlParser.scala:46)
	at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parsePlan(ParseDriver.scala:53)
	at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:582)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$.fetchObjectsOfSubject(Word2VecDataMaker.scala:118)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$firstTraverse$1.apply(Word2VecDataMaker.scala:141)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$firstTraverse$1.apply(Word2VecDataMaker.scala:141)
	at scala.collection.immutable.List.map(List.scala:288)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$.firstTraverse(Word2VecDataMaker.scala:141)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$6.apply(Word2VecDataMaker.scala:274)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$6.apply(Word2VecDataMaker.scala:274)
	at scala.collection.immutable.List.map(List.scala:288)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$.ceatingWord2VecCategoryData(Word2VecDataMaker.scala:274)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$.MakeWord2VecData(Word2VecDataMaker.scala:320)
	at tech.sda.arcana.spark.ExecuteOperations$.main(preprocessing.scala:34)
	at tech.sda.arcana.spark.ExecuteOperations.main(preprocessing.scala)
 */


/*
 * [Stage 329:====================================================>  (18 + 1) / 19]
                                                                                
Exception in thread "main" org.apache.spark.sql.catalyst.parser.ParseException: 
extraneous input '55.92722222222222' expecting {<EOF>, '.', '[', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'OR', 'AND', 'IN', NOT, 'BETWEEN', 'LIKE', RLIKE, 'IS', 'WINDOW', 'UNION', 'EXCEPT', 'INTERSECT', EQ, '<=>', '<>', '!=', '<', LTE, '>', GTE, '+', '-', '*', '/', '%', 'DIV', '&', '|', '^', 'SORT', 'CLUSTER', 'DISTRIBUTE', STRING}(line 1, pos 45)

== SQL ==
SELECT Object from triples where Subject = ""55.92722222222222"^^<http://www.w3.org/2001/XMLSchema#float>" and Object RLIKE  "(http://)" 
---------------------------------------------^^^

	at org.apache.spark.sql.catalyst.parser.ParseException.withCommand(ParseDriver.scala:197)
	at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parse(ParseDriver.scala:99)
	at org.apache.spark.sql.execution.SparkSqlParser.parse(SparkSqlParser.scala:46)
	at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parsePlan(ParseDriver.scala:53)
	at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:582)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$.fetchObjectsURIOfSubject(Word2VecDataMaker.scala:127)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$secondTraverse$1$$anonfun$apply$11.apply(Word2VecDataMaker.scala:156)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$secondTraverse$1$$anonfun$apply$11.apply(Word2VecDataMaker.scala:156)
	at scala.collection.immutable.List.map(List.scala:288)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$secondTraverse$1.apply(Word2VecDataMaker.scala:156)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$secondTraverse$1.apply(Word2VecDataMaker.scala:155)
	at scala.collection.immutable.List.foreach(List.scala:392)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$.secondTraverse(Word2VecDataMaker.scala:155)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$8.apply(Word2VecDataMaker.scala:285)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$$anonfun$8.apply(Word2VecDataMaker.scala:285)
	at scala.collection.immutable.List.map(List.scala:284)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$.ceatingWord2VecCategoryData(Word2VecDataMaker.scala:285)
	at tech.sda.arcana.spark.profiling.Dataset2Vec$.MakeWord2VecData(Word2VecDataMaker.scala:329)
	at tech.sda.arcana.spark.ExecuteOperations$.main(preprocessing.scala:33)
	at tech.sda.arcana.spark.ExecuteOperations.main(preprocessing.scala)
 * 
 */
