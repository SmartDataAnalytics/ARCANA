package tech.sda.arcana.spark.profiling
import edu.mit.jwi.Dictionary
import edu.mit.jwi.IDictionary 
import edu.mit.jwi.item.IIndexWord 
import edu.mit.jwi.item.ISynset
import edu.mit.jwi.item.IWord
import edu.mit.jwi.item.IWordID 
import edu.mit.jwi.item.POS 
import edu.mit.jwi.item.ISynsetID
import edu.mit.jwi.item.Pointer
import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException; 
//import java.util.ArrayList; 
import java.util.Iterator; 
//import java.util.List; 
import collection.JavaConverters._


import akka.dispatch.Foreach
import edu.smu.tspell.wordnet._
/*
 * An Object that is capable of extract Synsets form Wordnet using JWI & JAWS
 */
object WordNetJwi {
  def getSynonyms(dict:Dictionary,expression:String){
      val idxWord = dict . getIndexWord (expression , POS.NOUN) 
      val wordID = idxWord.getWordIDs().get (0) // 1 st meaning
      val word = dict.getWord(wordID) 
      val synset = word.getSynset() 
      val w = synset.getWords()
      val ITR = w.iterator()
      while(ITR.hasNext()) {
        println(ITR.next().asInstanceOf[IWord].getLemma)
      }
  }
       
  def getSynsets(word:String):scala.collection.mutable.Set[String]={
     var set = scala.collection.mutable.Set[String]()
     System.setProperty("wordnet.database.dir", AppConf.WordNetDict)
     
     val database: WordNetDatabase = WordNetDatabase.getFileInstance
     val synsets: Array[Synset] = database.getSynsets(word)
     
     for (i <- 0 until synsets.length) {
        //println("")
        val wordForms: Array[String] = synsets(i).getWordForms
        for (j <- 0 until wordForms.length) {
          //System.out.print((if (j > 0) ", " else "") + wordForms(j))
          set.add(wordForms(j))
        }
        //println(": " + synsets(i).getDefinition)
      }
     set
   }
  def getHypernyms(dict:Dictionary){
        // get the synset
        val idxWord = dict . getIndexWord ( " dog " , POS.NOUN ) ;
        val wordID = idxWord . getWordIDs () . get (0) ; // 1 st meaning
        val word = dict . getWord ( wordID ) ;
        var synset = word.getSynset();
        
        // get the hypernyms

        val hypernyms: List[ISynsetID] = synset.getRelatedSynsets(Pointer.HYPERNYM).asScala.toList
        // print out each h y p e r n y m s id and synonyms
        var words =  List[IWord]() 
        val ITR = hypernyms.iterator
        while(ITR.hasNext) {
        //println(ITR.next().asInstanceOf[IWord].getLemma)
        val sid = ITR.next().asInstanceOf[ISynsetID]
        val words = dict.getSynset(sid).getWords()
        var i: Iterator[IWord] = words.iterator()
        while (i.hasNext) {
          println(i.next().getLemma())
          }
        }

    }
  def main(args: Array[String]) = {
    // THIS USES JWI
      val url = new URL ( "file" , null , AppConf.WordNetDict ) 
      // construct the dictionary object and open it
      val dict = new Dictionary ( url ) 
      
      dict.open() 
      getSynonyms(dict,"capacity")
      //getHypernyms(dict)
      // look up first sense of the word " dog "
      /*
      val idxWord = dict . getIndexWord ( " dog " , POS.NOUN ) 
      val wordID = idxWord . getWordIDs () . get (0) 
      val word = dict . getWord ( wordID ) 
      println(" Id = " + wordID )
      println(" Lemma = " + word . getLemma () ) 
      println(" Gloss = " + word . getSynset () . getGloss () ) 
      */
        dict.close() 
        println("ResultsFromJAWS")
         getSynsets("kill").foreach(println)
      // THIS USES JAWS
        
  }
}