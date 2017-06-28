package tech.sda.arcana.spark.profiling
import scala.collection.JavaConversions._
import java.io.File
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
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
  // Token Sentences 
   def PTBTokenizer_Sentence(sentencete: String)= {

      val dp: DocumentPreprocessor = new DocumentPreprocessor(new StringReader(sentencete));
      for(sentence <- dp) { 
        println(sentence) 
        }  
}
  // This one is hosted on the site and works nicely ==> By token 
  def PTBTokenizer_Token(sentence: String): PTBTokenizer[CoreLabel] = {

    val ptbt: PTBTokenizer[CoreLabel] = new PTBTokenizer[CoreLabel](new StringReader(sentence), new CoreLabelTokenFactory(), "")

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
  
    // does combined work such as tokenization, lemmatization etc ...
      def lemmatize(documentText: String): List[String] = {
        
            val lemmas: List[String] = new LinkedList[String]()
               // Create an empty Annotation just with the given text
            val document: Annotation = new Annotation(documentText)
              // run all Annotators on this text
            this.pipeline.annotate(document)
              // Iterate over all of the sentences found
            val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation])
            for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
              // list of lemmas
              lemmas.add(token.get(classOf[LemmaAnnotation]))
            }
              // Retrieve and add the lemma for each word into the
              lemmas
        }
      def removeStopWords(Tokens: List[String]): List[String] = {
        val testWords = scala.io.Source.fromFile("src/main/resources/stopwords.txt").mkString
         // val lemmas2=lemmas.filterNot(elm => elm == data)
        Tokens.filter(!testWords.contains(_))
      }
  
  def main(args: Array[String]) = {

    /*
    val ptbt_Sentence = PTBTokenizer_Sentence("HEY THERE GUYS Home. Did you have a good time? ")
    val ptbt_Token = PTBTokenizer_Token("HEY THERE GUYS TERZ")
    
    while (ptbt_Token.hasNext) { 
      val label: CoreLabel = ptbt_Token.next() 
      println(label) }
    */
    
   val lemmas: List[String]  = lemmatize("How to plant a bomb in Traponia and execute everyone under the ground")
   val lemmasMod=removeStopWords(lemmas)
   
    lemmasMod.foreach { println }
    println("DONE")
  }
}


