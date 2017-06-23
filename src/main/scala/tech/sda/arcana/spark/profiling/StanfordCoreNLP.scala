package tech.sda.arcana.spark.profiling

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import java.io.StringReader
import java.util.Properties
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.pipeline.{ Annotation, StanfordCoreNLP }
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentClass
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.process.PTBTokenizer;


import java.util.Properties
import scala.collection.JavaConversions._ 
import scala.collection.immutable.ListMap
import scala.io.Source

import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
object Stanford {
  
  val properties = new Properties()
  //properties.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment")
  properties.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref, sentiment");
  properties.setProperty("ssplit.isOneSentence", "true")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(properties)
  val coreNLP = new StanfordCoreNLP(properties)
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
  
  // This one is hosted on the sit and works nicely 
  def PTBTokenizer(sentence: String): PTBTokenizer[CoreLabel] = {

    val ptbt: PTBTokenizer[CoreLabel] = new PTBTokenizer[CoreLabel](new StringReader("HEY THERE GUYS"), new CoreLabelTokenFactory(), "")

    ptbt
}
  
  // This one also works nicely and you can add some more functionality, the normalizeToken compliment this one 
  def tokenize(s: String)  = { 

            val annotation = new Annotation(s)
            coreNLP.annotate(annotation)
            annotation.get(classOf[TokensAnnotation]).map(_.toString)
                       
          }
  def normalizeToken(t: String) = {
    val ts = t.toLowerCase
    val num = "[0-9]+[,0-9]*".r
    ts match {
      case num() => "NUMBER"
      case _ => ts
    }
}
  def main(args: Array[String]) = {
 
     //val dp: DocumentPreprocessor = new DocumentPreprocessor("Hey let us go and play") 
     //for (sentence <- dp) { println(sentence) }
    
    //val ptbt: PTBTokenizer[CoreLabel] = new PTBTokenizer[CoreLabel](new StringReader("HEY THERE GUYS"), new CoreLabelTokenFactory(), "")
    val ptbt = PTBTokenizer("HEY THERE GUYS")
    
    while (ptbt.hasNext) { 
      val label: CoreLabel = ptbt.next() 
      println(label) }
    
    //val test = tokenize("Hi Hi Let's me go")
    //   for ( x <- test ) {
    //     println( x )
    //  }
    //println(test)
    println("DON")
  }
}


