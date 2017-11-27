package tech.sda.arcana.spark.profiling

import java.io.File
import java.nio.charset.Charset
import java.util.Properties
import org.apache.spark.sql.DataFrame


import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.functions._

import com.google.common.io.Files
import edu.stanford.nlp.process.CoreLabelTokenFactory
import edu.stanford.nlp.ling.CoreAnnotations.{PartOfSpeechAnnotation, SentencesAnnotation, TextAnnotation, TokensAnnotation}
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TextAnnotation, TokensAnnotation}

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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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

 def queryQuestionOnDbpedia(){
      val sc = spark.sparkContext
      import spark.implicits._
      val lines = sc.textFile("/home/elievex/Repository/ExtResources/sentences")
      val DF=lines.toDF()
      DF.createOrReplaceTempView("triples")
      val word="Achtung"
      val REG = raw"(?<![a-zA-Z])$word(?![a-zA-Z])".r
      val Res = spark.sql(s"SELECT * from triples where value RLIKE '$REG' ")
      Res.show()
 }
 
 def consultDbpediaSpotlight(){
     import dbpediaSpotlight.spotlight
     import org.json.simple.{JSONArray,JSONObject}

      val DBpEquivalent: JSONArray = spotlight.getDBLookup("germany", "0.0")
      val obj2: JSONObject = DBpEquivalent.get(0).asInstanceOf[JSONObject]
      println(obj2.get("uri").toString)
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
  // Process question and fill it as an object//:List[Token]=
  def ProcessSentence(text:String):(List[Token],String)={
        val extractNumber = raw"(\d+)".r
        var tokens = new ListBuffer[Token]()
        var posTagString = ""
        val text = "How to kill an animal?"

        // create blank annotator
        val document: Annotation = new Annotation(text)
    
        // run all Annotator - Tokenizer on this text
        pipeline.annotate(document)
    
        val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList
    
      val Tokens=  (for {
          sentence: CoreMap <- sentences
          token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
          word: String = token.get(classOf[TextAnnotation])
          pos: String = token.get(classOf[PartOfSpeechAnnotation])
          lemma: String = token.get(classOf[LemmaAnnotation])
    
        } yield (token, word, pos, lemma)) 
        Tokens.foreach{t => 
            tokens+=new Token(extractNumber.findFirstIn(t._1.toString()).getOrElse("0"),t._2.toLowerCase(),t._3,t._4.toLowerCase())
            var temp=""
            if(t._3.toString()=="NN"||t._3.toString()=="NNS"||t._3.toString()=="NNS"||t._3.toString()=="NNP"||t._3.toString()=="NNPS"){
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
   
   
  def main(args: Array[String]) = {

    // match number (?<=\D)\d+(?=\D)

    val question = "hello how can i go to the next airport?".toLowerCase()
    
    val questionInfo = ProcessSentence(question)
    val questionObj = new QuestionSentence(question,removeStopWords(stringToDF(question)),sentiment(question),questionInfo._1,questionInfo._2)
    
    questionObj.tokens.foreach(t=>println(t.index+" "+t.word+" "+t.posTag+" "+t.lemma))
    println(questionObj.PosSentence)
    
    val extractCase = raw"V\s(.+)?N\s+".r
    println(extractCase.findFirstIn(questionObj.PosSentence).getOrElse("0"))
    //| Old Way
    //> tokenizeQuestionWithRegex("Hi There How are you? There was a car walking by a dog nearby the horse?")
    spark.stop()
  }
  
}

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
