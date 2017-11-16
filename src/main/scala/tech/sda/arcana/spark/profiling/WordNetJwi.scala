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

object WordNetJwi {
  def getSynonyms(dict:Dictionary){
    // look up first sense of the word " dog "
      val idxWord = dict . getIndexWord ( " dog " , POS . NOUN ) ;
      val wordID = idxWord . getWordIDs () . get (0) ; // 1 st meaning
      val word = dict . getWord ( wordID ) ;
      val synset = word . getSynset () ;
      val w = synset.getWords()
      val ITR = w.iterator()
      while(ITR.hasNext()) {
        println(ITR.next().asInstanceOf[IWord].getLemma)
      }
  }
  
  def getHypernyms(dict:Dictionary){
        // get the synset
        val idxWord = dict . getIndexWord ( " dog " , POS . NOUN ) ;
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
      val url = new URL ( "file" , null , "src/WordNet/3.0/dict" ) 
      // construct the dictionary object and open it
      val dict = new Dictionary ( url ) 
      
      dict.open() 
      //getSynonyms(dict)
      getHypernyms(dict)
      // look up first sense of the word " dog "
      val idxWord = dict . getIndexWord ( " dog " , POS . NOUN ) 
      val wordID = idxWord . getWordIDs () . get (0) 
      val word = dict . getWord ( wordID ) 
      println(" Id = " + wordID )
      println(" Lemma = " + word . getLemma () ) 
      println(" Gloss = " + word . getSynset () . getGloss () ) 
        dict.close() 
  }
}