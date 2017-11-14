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
  
    val props: Properties = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, parse, sentiment")
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
  def removeStopWords(DF: DataFrame):DataFrame={
    val remover = new StopWordsRemover()
          .setInputCol("row")
          .setOutputCol("filtered")
    val nDF=remover.transform(DF)
    nDF
  }
   case class Sentence(sentence: String)
  def main(args: Array[String]) = {
    import spark.implicits._
    val props: Properties = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

    val text = "Hi There How are you There was a car walking by a dog nearby the horse?".toLowerCase()
    //he writes that and she is writing it now he was walking and she walks today there?
    //Hi There How are you There was a car walking by a dog nearby the horse?
    val DF=removeStopWords(stringToDF(text))
    DF.show(false)
    val list=DF.select("filtered").rdd.map(r => r(0)).collect()
    println(list(0))
    val tito=list.mkString(",")
   // println(DF.select(concat_ws(",", "filtered"))
   // val mkString = udf((arrayCol:Array[String])=>arrayCol.mkString(","))  

    //val T=DF.select(("filtered")).first.getString(0)

    //println(T)
    
    val mkStringz = udf((arrayCol:Seq[String])=>arrayCol.mkString(",")) 
    
    val dfWithString=DF.select($"filtered").withColumn("arrayString",
          mkStringz($"filtered"))  
    
    
          dfWithString.show()
     val T=dfWithString.select(("arrayString")).first.getString(0)    
     println(T)
    
     println(T.replace(',', ' '))
    val mi=DF.select(concat_ws(",",$"filtered"))
    //println(mi.toString())
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
