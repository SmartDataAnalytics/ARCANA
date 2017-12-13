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
  def expressionCheck(questionObj:QuestionObj):Int={
    val extractCase = raw"V\s(.+)?N\s?".r
    //println(questionObj.PosSentence)
    var flag=0
    //| check whether there is combination of V followed by N
    if(extractCase.findFirstIn(questionObj.PosSentence).getOrElse("0")!="0"){
      var verbs = new ListBuffer[Token]()
      var nouns = new ListBuffer[Token]()
      
      //| Add only V and N tokens to buffers
      for(token<-questionObj.tokens){
        //print(token.posTag+"-")
        if(token.posTag=="NN" ||token.posTag=="NNS"||token.posTag=="NNP" ||token.posTag=="NNPS"){
          nouns += token
        }
        if(token.posTag=="VBZ" ||token.posTag=="VB"||token.posTag=="VBD" ||token.posTag=="VBG"||token.posTag=="VBN" ||token.posTag=="VBP"){
          verbs += token
        }
      }
      //| Get DB Info
      val DF = AppDBM.readDBCollection(AppConf.secondPhaseCollection)
      DF.createOrReplaceTempView("DB")

      //| check if Verb is present in DB
      verbs.foreach{
        t=>val word = t.lemma
        val res = spark.sql(s"SELECT distinct relationshipID FROM DB where verb = '$word' ")
        val rellist = res.select("relationshipID").rdd.map(r => r(0).asInstanceOf[Int]).collect()
        if(!rellist.isEmpty){
          flag=1
          t.relationID=rellist(0)
        }
      }
     if(flag==1){
       //| check if Noun is present in DB
      nouns.foreach{
        t=>val word = t.lemma
        val res = spark.sql(s"SELECT distinct relationshipID FROM DB where noun = '$word' ")
        val rellist = res.select("relationshipID").rdd.map(r => r(0).asInstanceOf[Int]).collect()
        if(!rellist.isEmpty){
          flag=2
          t.relationID=rellist(0)
        }
      }
     }
     //nouns.foreach(t=>println(t.word+" "+t.relationID))

     //| Cross the results and see if verb and noun are from the same relationship if yes its bad \\ ofcourse there is a place to improve this for example by checking if there is a CC (or / and) between the verb and noun
     if(flag==2){
       verbs.foreach{
        t=>nouns.foreach{u=>
          if(t.relationID==u.relationID)
          {
            flag=3 // malicious 
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
        for (i <- 0 to DBpEquivalent.size()-1) {
            val obj2: JSONObject = DBpEquivalent.get(i).asInstanceOf[JSONObject]
            result+=obj2.get("uri").toString
        }
      }
      result
 }
  def fetchTokenUris(word:String,path:String):Set[String]={
    val listBuff=consultDbpediaSpotlight(word)
    
    val myDF=RDFApp.readProcessedData(path)
    val T1=Dataset2Vec.fetchAllOfWordAsOubject(myDF,word)
    val T2=Dataset2Vec.fetchAllOfWordAsSubject(myDF,word)
    var s : Set[String] = Set()
    T1.foreach(f=>s+=f.Uri)
    T2.foreach(f=>s+=f.Uri)
    listBuff.foreach(f=>s+=f)
    s
  }
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
   def extractMostExactSenti(uriString:String,posString:String,DF:DataFrame){
     
    var resultList : List[(String,Double)] = List()
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
      //allDB.show()
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
          println("SCORE")
    println(mapIndicator(sentiScore,sentiIndicator),sentiIndicator)
    }
   }
   // Process question and fill it as an object//:List[Token]=
  def ProcessSentence(text:String,path:String,DF:DataFrame):(List[Token],String)={
        val extractNumber = raw"(\d+)".r
        var tokens = new ListBuffer[Token]()
        var posTagString = ""
        val collectionDF=DF
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
          if(!listOfLines.contains(t._2)){
            val list = fetchTokenUris(t._2,path)
            tokens+=new Token(extractNumber.findFirstIn(t._1.toString()).getOrElse("0"),t._2.toLowerCase(),t._3,t._4.toLowerCase(),0, List())
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
  def processQuestion(input:String,path:String,DF:DataFrame): QuestionObj = {
    //QuestionObj(sentence:String,sentenceWoSW:String,SentimentExtraction:Int,tokens:List[Token],PosSentence:String,var phaseTwoScore:Int)
    //Token(index:String,word:String,posTag:String,lemma:String,var relationID:Int)
 
    val question = input.toLowerCase()
    val questionInfo = ProcessSentence(question,path,DF)
    val questionObj = new QuestionObj(question,removeStopWords(stringToDF(question)),sentiment(question),questionInfo._1,questionInfo._2,0)
    questionObj.phaseTwoScore=expressionCheck(questionObj)
 
    questionObj
  }
  def readQuestions(path:String):RDD[String]={
    val sc = spark.sparkContext
    val textFile = sc.textFile(path)
    val noEmptyRDD = textFile.filter(x => (x != null) && (x.length > 0))
    println("~Reading Questions is done~")
    noEmptyRDD
  }

  def main(args: Array[String]) = {
    
    val path = "/home/elievex/Repository/resources/"

    // Read the Questions
    val noEmptyRDD=ProcessQuestion.readQuestions(path+AppConf.Questions)
    
    // Process Questions
    val ds= noEmptyRDD.map(t=>ProcessQuestion.processQuestion(t,"/home/elievex/Repository/resources/",AppDBM.readDBCollection(AppConf.firstPhaseCollection))).toDS().cache()
    println("~Processing Questions is done~")
    ds.show(false)
    
    //consultDbpediaSpotlight()
    spark.stop()
  }
  
}

