package tech.sda.arcana.spark.profiling

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.pipeline.{ Annotation, StanfordCoreNLP }
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentClass
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations


object Stanford {
  
  val properties = new Properties()
  //properties.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment")
  properties.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref, sentiment");
  properties.setProperty("ssplit.isOneSentence", "true")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(properties)

  def Sentiment(sentence: String): String = {

    // create an empty Annotation just with the given text
    val document: Annotation = new Annotation(sentence)

    // run all Annotators on this text
    pipeline.annotate(document)

    val sentiment: String = document.get(classOf[SentencesAnnotation]).get(0).get(classOf[SentimentClass])

    sentiment
  }

  def Dependencies(sentence: String): String = {

    // create an empty Annotation just with the given text
    val document: Annotation = new Annotation(sentence)

    // run all Annotators on this text
    pipeline.annotate(document)

    val sentences: CoreMap = document.get(classOf[SentencesAnnotation]).get(0)

    val dependencies = sentences.get(classOf[CollapsedCCProcessedDependenciesAnnotation])

    dependencies.toString()
}

  def main(args: Array[String]) = {
    
    val test = Sentiment("Milk")
    println(test)
  }

}
