package tech.sda.arcana.spark.profiling

import java.io.File
import java.nio.charset.Charset
import java.util.Properties
import org.apache.spark.sql.DataFrame


import scala.collection.JavaConverters._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat_ws

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
    
  def getSentiment(sentiment: Int): String = sentiment match {
    case x if x == 0 || x == 1 => "Negative"
    case 2 => "Neutral"
    case x if x == 3 || x == 4 => x.toString()
  }
 def getScore(){
    val text1 = "That was very amazing"
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
 
   @transient private var sentimentPipeline: StanfordCoreNLP = _

  private def getOrCreateSentimentPipeline(): StanfordCoreNLP = {
    if (sentimentPipeline == null) {
      val props = new Properties()
      props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
      sentimentPipeline = new StanfordCoreNLP(props)
    }
    sentimentPipeline
  }
   
 def sentiment (sentence: String) { 

   //Measures the sentiment of an input sentence on a scale of 0 (strong negative) to 4 (strong positive).
    val pipeline = getOrCreateSentimentPipeline()
    val annotation = pipeline.process(sentence)
    val tree = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      .asScala
      .head
      .get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
    val x = RNNCoreAnnotations.getPredictedClass(tree)
    println(x)
     }
 
 
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
  def PosTagger(){
    val text = "Quick brown fox jumps over the lazy dog. This is Harshal."

    // create blank annotator
    val document: Annotation = new Annotation(text)

    // run all Annotator - Tokenizer on this text
    pipeline.annotate(document)

    val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList

    (for {
      sentence: CoreMap <- sentences
      token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
      word: String = token.get(classOf[TextAnnotation])
      pos: String = token.get(classOf[PartOfSpeechAnnotation])

    } yield (token, word, pos)) foreach(t => println("token: " + t._1 + " word: " +  t._2 + " pos: " +  t._3))

  }
  def Lemmatizer():List[Token]={

        var tokens = new ListBuffer[Token]()

        val text = "Quick brown fox jumps over the lazy dog. This is Harshal."

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
    
        } yield (token, word, pos, lemma)) //foreach(t => println("token: " + t._1 + " word: " +  t._2 + " pos: " +  t._3 +  " lemma: " + t._4))
        //  case class Token(index:Int,word:String,posTag:String,lemma:String)
      Tokens.foreach(t => tokens+=new Token(t._1.toString(),t._2.toLowerCase(),t._3,t._4.toLowerCase())     )
      tokens.toList
  }
   case class Sentence(sentence: String)
   
  def main(args: Array[String]) = {

    // match number (?<=\D)\d+(?=\D)
     
    val question = "hello how can i go to the next airport?".toLowerCase()
    //he writes that and she is writing it now he was walking and she walks today there?
    //Hi There How are you There was a car walking by a dog nearby the horse?
    
     val questionObj = new QuestionSentence(question,removeStopWords(stringToDF(question)),"",Lemmatizer())
    
   questionObj.tokens.foreach(t=>println(t.index+" "+t.word+" "+t.posTag+" "+t.lemma))
 
    //| Removing Stop Words
    //>println(removeStopWords(stringToDF(text)))
    
   
/*
    // create blank annotator
    val document: Annotation = new Annotation(text)

    // run all Annotator - Tokenizer on this text
    pipeline.annotate(document)

    val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList

    (for {
      sentence: CoreMap <- sentences
      token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
      word: String = token.get(classOf[TextAnnotation])

    } yield (word, token)) foreach(t => println("word: " + t._1 + " token: " +  t._2))

*/


   /*
    // create blank annotator
    val document: Annotation = new Annotation(text)

    // run all Annotator - Tokenizer on this text
    pipeline.annotate(document)

    val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList

    val tokens=(for {
      sentence: CoreMap <- sentences
      token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
      word: String = token.get(classOf[TextAnnotation])
      pos: String = token.get(classOf[PartOfSpeechAnnotation])
      lemma: String = token.get(classOf[LemmaAnnotation])

    } yield (token, word, pos, lemma)) 

    tokens.foreach(t => println("token: " + t._1 + " word: " +  t._2 + " pos: " +  t._3 +  " lemma: " + t._4))
  	*/
    spark.stop()
  }
  
}

/*
     val text = "Quick brown fox jumps over the lazy dog. This is Harshal."
    val tst="How to kill an animal?"
    sentiment(tst)
    getScore()
    val document: Annotation = new Annotation(text)
    pipeline.annotate(document)
    

       val R = new CoreLabel()
        R.setWord("Play")
        
        println("This: "+R.get(classOf[PartOfSpeechAnnotation]))
        
        val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList
    
        (for {
          sentence: CoreMap <- sentences
          token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
          word: String = token.get(classOf[TextAnnotation])
          pos: String = token.get(classOf[PartOfSpeechAnnotation])
    
        } yield (token, word, pos)) foreach(t => println("token: " + t._1 + " word: " +  t._2 + " pos: " +  t._3))
   getScore()
   println("DONE")
 */
