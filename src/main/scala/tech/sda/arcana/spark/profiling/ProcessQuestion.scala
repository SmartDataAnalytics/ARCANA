package tech.sda.arcana.spark.profiling

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
 
//import scala.collection.parallel.ParIterableLike.Foreach

/*
 * An Object that make use of stanford.nlp to perform operations on the text
 */
//https://stackoverflow.com/questions/1833252/java-stanford-nlp-part-of-speech-labels
object ProcessQuestion {
     val spark = SparkSession.builder
      .master("local[*]")
      .config(AppConf.inputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
      .config(AppConf.outputUri, AppConf.host + AppConf.dbName + "." + AppConf.firstPhaseCollection)
      .appName("Dataset2Vec")
      .getOrCreate()
    import spark.implicits._
    
    val props: Properties = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
    
  /** Read the file that has the questions 
  *   @param path to the file
  *   @return Question as an RDD
  */
  def readQuestions(path:String):RDD[String]={
    val sc = spark.sparkContext
    val textFile = sc.textFile(path)
    val noEmptyRDD = textFile.filter(x => (x != null) && (x.length > 0))
    println("~Reading Questions is done~")
    noEmptyRDD
  }
 // Simple Question Tokenizing
  def tokenizeQuestion(question: String){
    /*
     * in case of many question you can do:
      val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
  	  )).toDF("id", "sentence")
     */
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, question)
    )).toDF("id", "sentence")
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val countTokens = udf { (words: Seq[String]) => words.length }
    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("sentence", "words")
        .withColumn("tokens", countTokens(col("words"))).show(false)
    removeStopWordDfs(tokenized)
  }
  // Question Tokenizing while using Regex
  def tokenizeQuestionWithRegex(question: String){
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, question)
    )).toDF("id", "sentence")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)
    val countTokens = udf { (words: Seq[String]) => words.length }
    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("sentence", "words")
        .withColumn("tokens", countTokens(col("words")))
        
    removeStopWordDfs(regexTokenized)
  }  
 def removeStopWordDfs (DF: DataFrame){
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")
    
    remover.transform(DF).show(false)
  }
 //////////////////////////////////////////////////////////////////////////////////
  def getSentiment(sentiment: Int): String = sentiment match {
    case x if x == 0 || x == 1 => "Negative"
    case 2 => "Neutral"
    case x if x == 3 || x == 4 => x.toString()
  }
 def getScore(){
    val text1 = "How to run faster than light?"
    val text2 = "Barack Obama is leaving Presidency. Donald Trump will be President of USA."

    // create blank annotator
    val document1: Annotation = new Annotation(text1)
    val document2: Annotation = new Annotation(text2)

    // run all Annotators
    pipeline.annotate(document1)
    pipeline.annotate(document2)


    val sentences1: List[CoreMap] = document1.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.toList
    val sentences2: List[CoreMap] = document2.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.toList

    // Check if positive sentiment sentence is truly positive
    sentences1
      .map(sentence => (sentence, sentence.get(classOf[SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, getSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .foreach(println)

    // Check if the negative sentiment sentence is truly negative
    sentences2
      .map(sentence => (sentence, sentence.get(classOf[SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, getSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .foreach(println)
 }


 // SentiWord of entire sentence
 def sentiment (sentence: String):Int= { 

   //Measures the sentiment of an input sentence on a scale of 0 (strong negative) to 4 (strong positive).
 //   val pipeline = getOrCreateSentimentPipeline()
    val annotation = pipeline.process(sentence)
    val tree = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      .asScala
      .head
      .get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
    val x = RNNCoreAnnotations.getPredictedClass(tree)
    x
     }
 
  // Convert String to DF
  def stringToDF(sentence: String):DataFrame={
       val dataSet = spark.createDataFrame(Seq(
          (0, sentence.split(" ").toSeq)
        )).toDF("id", "row")
        dataSet
  }
  
  // remove stop words and return the resulting dataframe as a string 
  def removeStopWords(DF: DataFrame):String={
    val remover = new StopWordsRemover().setInputCol("row").setOutputCol("filtered")
    val nDF=remover.transform(DF)
    val mkStringz = udf((arrayCol:Seq[String])=>arrayCol.mkString(",")) 
    
    val dfWithString=nDF.select($"filtered").withColumn("arrayString",mkStringz($"filtered")) 
    val T=dfWithString.select(("arrayString")).first.getString(0) 
    val sentence = T.replace(',', ' ')
    
    sentence
  }   
  def expressionCheck(questionObj:QuestionObj,DFDB2:DataFrame):Int={ // 2 or 3 = BLOCK
    val extractCase = raw"V\s(.+)?N\s?".r
    //println(questionObj.PosSentence)
    var flag=0
    //| check whether there is combination of V followed by N
    if(extractCase.findFirstIn(questionObj.PosSentence).getOrElse("0")!="0"){
      var verbs = new ListBuffer[Token]()
      var verbsLiteral = new ListBuffer[String]()
      var nouns = new ListBuffer[Token]()
      var nounsLiteral = new ListBuffer[String]()
      //| Add only V and N tokens to buffers
      for(token<-questionObj.tokens){
        //print(token.posTag+"-")
        if(token.posTag=="NN" ||token.posTag=="NNS"||token.posTag=="NNP" ||token.posTag=="NNPS"){
          nouns += token
          nounsLiteral +=token.lemma
        }
        if(token.posTag=="VBZ" ||token.posTag=="VB"||token.posTag=="VBD" ||token.posTag=="VBG"||token.posTag=="VBN" ||token.posTag=="VBP"){
          verbs += token
          verbsLiteral+=token.lemma
        }
      }
      //| Get DB Info
      //val DF = AppDBM.readDBCollection(AppConf.secondPhaseCollection)
      val DF = DFDB2
      DF.createOrReplaceTempView("DB")

      //| check if Verb is present in DB
      verbs.foreach{
        t=>val word = t.lemma
        val res = spark.sql(s"SELECT distinct relationshipID FROM DB where verb = '$word' ")
        // get relationID of the verb
        val rellist = res.select("relationshipID").rdd.map(r => r(0).asInstanceOf[Int]).collect()
        if(!rellist.isEmpty){
          flag=1
          t.relationID=rellist(0)
          var relID=t.relationID
          // Get all nouns
          val resNoun = spark.sql(s"SELECT distinct noun FROM DB where relationshipID = '$relID' ")
          val UriList=resNoun.select("noun").rdd.map(r => r(0).asInstanceOf[String]).collect()
          // Are the nouns in the question found in the same relation?
          nounsLiteral.foreach{
            f => if(UriList.contains(f)){
              flag=2
            }
          }
          // are compound nouns found in the question?
          if(flag==1 && flag!=2){
            UriList.foreach{
            f => 
              if(questionObj.sentence contains f){
              flag =3
              //println("FOUND EM OB")
              break
             }
            }
          }
        }
      }
    }
      flag
  }
   def consultDbpediaSpotlight(word:String):ListBuffer[String]={
     import dbpediaSpotlight.spotlight
     import org.json.simple.{JSONArray,JSONObject}

     var result = new ListBuffer[String]()
     val DBpEquivalent: JSONArray = spotlight.getDBLookup(word, "0.0")
      if(DBpEquivalent.size()>0){
        println("Nonempty")
        for (i <- 0 to DBpEquivalent.size()-1) {
            val obj2: JSONObject = DBpEquivalent.get(i).asInstanceOf[JSONObject]
            result+=obj2.get("uri").toString
        }
      }
     println("empty")
      result
 }
  /** Fetch the tokens of a uri 
    */
  def fetchTokenUris(word:String,DFpedia:DataFrame,modelvec: Word2VecModel):Set[String]={
    // Fetching URIs of word
    val T1=Dataset2Vec.fetchAllOfWordAsSubject(DFpedia,word)
    var s : Set[String] = Set()
    T1.foreach(f=>s+=f.Uri)
    // Fetching Uris from Word2Vec
    T1.foreach{
      f=> var synSet=Word2VecModelMaker.getWord2VecSynonyms(modelvec,f.Uri,10)
      synSet.foreach{syn=>
       s+=syn.word
      }  
    }
    s
  }
   /** Map between stanford and sentword notations 
    */
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
   /** a function that tries to find other scores if the one required  is -9
    */
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
   /** Simple mapping of the object properties and an inidcator
    */
   def mapIndicator(obj:DBRecord,indicator:String):Double={
     var score = 0.0
     //println(indicator)
     //println(obj.senti_uclassify)
      indicator match {
        case "senti_v"  => score = obj.senti_v
        case "senti_n"  => score = obj.senti_n
        case "senti_r"  => score = obj.senti_r
        case "senti_a"  => score = obj.senti_a
        case "senti_uclassify"  => score = obj.senti_uclassify
        }
     score
   }
   /** Process the Uris' of a token
    *   @param list of Uris, Pos tag of word 
    *   @return Uri and its general score 
    */
   def getTokenUrisSentiScore(myList:List[String],posString:String,DFDB1:DataFrame):ListBuffer[(String,Double)]={
     var UrisScore = new ListBuffer[(String,Double)]()
     val DF = DFDB1 //.select("category","cosine_similary","expression","relatedTo","senti_a","senti_n","senti_r","senti_uclassify","senti_v","uri").distinct()
     //val DF = AppDBM.readDBCollection(AppConf.firstPhaseCollection)//.select("category","cosine_similary","expression","relatedTo","senti_a","senti_n","senti_r","senti_uclassify","senti_v","uri").distinct()
     //val distinctDF= DF.select("category","cosine_similary","expression","relatedTo","senti_a","senti_n","senti_r","senti_uclassify","senti_v","uri").distinct()
     DF.createOrReplaceTempView("DB")

     myList.foreach{uriString=>
       
       var resultList : List[(String,Double)] = List()
       var finalResult = -99d
       var foundFlag=false
       var myVoteScore = new ListBuffer[(DBRecord,String)]()
       val dbPOS=stanfordPOS2sentiPos(posString)
       val sentiArray = Array("senti_n","senti_v","senti_r","senti_a","senti_uclassify")
       //| I did this because if I searched for the sentiment analysis of a POS and it wasn't found I will need to query the DB again to fetch an alternative 
       //| by doing this if I didn't find the score I want I can get it from the other without querying the data again  
       var sentiScore=new DBRecord("","","",0.0,0.0,0.0,0.0,0.0,"",0.0)
       var sentiIndicator=""
       // Grabbing all info about URI
       val allDB = spark.sql(s"""SELECT  * FROM DB where uri = "$uriString"  """) 
       if(allDB.count()>0){
         allDB.createOrReplaceTempView("QuestionURI")
         // Grabbing the required POS 
         val specDB = spark.sql(s"""SELECT * FROM DB WHERE (uri,$dbPOS) IN ( SELECT  uri,min($dbPOS) FROM QuestionURI where uri = "$uriString" group by(uri))""")
         if(specDB.count()>0){
           //specDB.show()
           val result = specDB.as[DBRecord].collect()
           // IF IT IS -9 we find another
           if(isScoreNine(result(0),dbPOS)){
               sentiArray.foreach{x=>
                 if(x!=dbPOS){
                    val sentiDB = spark.sql(s"""SELECT * FROM DB WHERE (uri,$x) IN ( SELECT  uri,min($x) FROM QuestionURI where uri = "$uriString" group by(uri))""")
                    val sentiResult = sentiDB.as[DBRecord].collect()
                    if(!isScoreNine(sentiResult(0),x)){
                       myVoteScore+=((sentiResult(0),x)) 
                    }
                 }
               }
               // if not -9 take it already
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
        UrisScore+=((uriString,finalResult))
     }
     UrisScore
   }
 
 /** Calculate the sentiment score of the complete question 
  *   @param token of question 
  *   @return summary of findings
  */
  def calcFinalScore(tokenList:List[Token]):String={
     
     var totalScoreUris=0
     var totalFoundUris=0
     val tokenNum = tokenList.size
     var TokenSentiScore=0.0d
     var TotalTokenSentiScore=0.0d

     // Loop the tokens
       tokenList.foreach{t=>       
         var counter = 0 
         var sum = 0.0
         var totalScoreTokenUris=0
         if(t.uriTuple.size>0){
           // Loop the URIS
           t.uriTuple.foreach{f=>
              if(f._2!= -99){
                sum+=f._2
                counter+=1
                totalFoundUris+=1
                totalScoreUris+=1
                totalScoreTokenUris+=1
              }else if(f._2== -99){
                totalFoundUris+=1
                //totalScoreTokenUris+=1
                //counter+=1
              }
           }
           t.tokenUrisSentiScore += (if ((sum/totalScoreTokenUris.toDouble).isNaN) 0.0 else (sum/totalScoreTokenUris.toDouble) )
           TotalTokenSentiScore+=t.tokenUrisSentiScore
         }
       }
     //}
     val summary="We found a total of "+totalFoundUris+ " Uris related to this question, "+totalScoreUris+" of them are present in the DB, with a total Sentiment score of "+(if ((TotalTokenSentiScore/tokenNum.toDouble).isNaN) 0.0 else (TotalTokenSentiScore/tokenNum.toDouble) )
     //println("Found in DB "+totalScoreUris+" from a total number of Uris: "+totalFoundUris)
     summary
   }
   
   
 /** Process the question on the token level
  *   @param Question, path to resources 
  *   @return list of token and posTagString
  */
  def ProcessSentence(text:String,path:String,DFpedia:DataFrame,DFDB1:DataFrame):(List[Token],String)={
    val Word2VecModel = Word2VecModelMaker.loadWord2VecModel(path+AppConf.Word2VecModel)
      val extractNumber = raw"(\d+)".r
      var tokens = new ListBuffer[Token]()
      var posTagString = ""
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
    
        } yield (token, word, pos, lemma)) 
        Tokens.foreach{t => 
          //Removing stop words before operating on a token
          if(!listOfLines.contains(t._2)){
            // fetching URIS of each token(lemma)
            val list = fetchTokenUris(t._4,DFpedia,Word2VecModel)
            var scoredUrisList=  List[(String,Double)]()
            // Sentiment Score
            if(list.size>0){
              //DB required Columns in here
              scoredUrisList=getTokenUrisSentiScore(list.toList,t._3.toString(),DFDB1).toList
            }else{
              scoredUrisList=List()
            }
            tokens+=new Token(extractNumber.findFirstIn(t._1.toString()).getOrElse("0"),t._2.toLowerCase(),t._3,t._4.toLowerCase(),0, scoredUrisList,0.0)
          }
          //var temp=""
            if(t._3.toString()=="NN"||t._3.toString()=="NNS"||t._3.toString()=="NNP"||t._3.toString()=="NNPS"){
              posTagString+="N"+" "
            }else if(t._3.toString()=="VBZ"||t._3.toString()=="VB"||t._3.toString()=="VBD"||t._3.toString()=="VBG"||t._3.toString()=="VBN"||t._3.toString()=="VBP"){
              posTagString+="V"+" "
            } else {
              posTagString+=t._3.toString()+" "
            }
          }
        //tokens.toList
      (tokens.toList, posTagString)
  }
 /** process the question by fetching different infromation after applying different routines on it 
  *   @param Question, path to resources 
  *   @return Processed Question Object 
  */
  def processQuestion(input:String,path:String,DF:DataFrame,DFDB1:DataFrame,DFDB2:DataFrame): QuestionObj = {
    val question = input.toLowerCase()
    val questionInfo = ProcessSentence(question,path,DF,DFDB1)
    var summary = calcFinalScore(questionInfo._1)
    val questionObj = new QuestionObj(question,removeStopWords(stringToDF(question)),sentiment(question),questionInfo._1,questionInfo._2,0,summary)
    questionObj.phaseTwoScore=expressionCheck(questionObj,DFDB2)
    questionObj
  }

  def main(args: Array[String]) = {

    //val myList = List("<http://simple.dbpedia.org/resource/Category:Armenian_military>","<http://simple.dbpedia.org/resource/Detachment_(military)>")
    //var list = getTokenUrisSentiScore(myList,"NN")
    //list.foreach(println)
  }
  
}
//QuestionObj(sentence:String,sentenceWoSW:String,SentimentExtraction:Int,tokens:List[Token],PosSentence:String,var phaseTwoScore:Int)
//Token(index:String,word:String,posTag:String,lemma:String,var relationID:Int)
    


//SELECT  * FROM DB where uri = '<http://dbpedia.org/resource/Stronger_(What_Doesn't_Kill_You)>'  
