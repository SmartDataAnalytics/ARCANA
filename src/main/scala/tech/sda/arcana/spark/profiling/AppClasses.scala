package tech.sda.arcana.spark.profiling
import scala.collection.mutable.ListBuffer

/*
 * A scala file that has all classes used in the project 
 */

class RDFURI(var Uri: String) {
   //var uris = new ListBuffer[RDFURI]()
   //var URIslist = List.newBuilder[URI]
   var URIslist  = List[RDFURI]()
   var FormedURI = ""
}

class Category (var Category: String,var uri:  List[RDFURI] ) {
  var FormedURI = ""
}
  case class X(_id: Int, _expression: String, indices: List[Integer], weights: Array[Double])
  case class Record(_id: Int, expression: String, rank: Double, rsc: List[String])
  case class Triple(Subject:String, Predicate:String, Object:String)
  case class Synonym(word: String, similarity: Double)
  case class OldDBRecord(_id: Int, uri: String, expression: String, category: String, score: Double, weight: Double, objectOf: String) // score = cosine similary - weight = sentiment analysis
  case class DBRecord(uri: String, expression: String, category: String, senti_n: Double, senti_v: Double, senti_a: Double, senti_r: Double, senti_uclassify: Double, relatedTo: String, cosine_similary: Double)
  //n for noun files, v for verb files, a for adjective files, r for adverb files.
  // for SentiWordFile
  case class SentiWordNetClass(POS:String, ID:String, PosScore:String, NegScore:String, SynsetTerms:String)
  case class SentiWordSpark(POS:String, ID:String, PosScore:String, NegScore:String, Term:String,TermRank:String)
  // for SentiWordFeedbackFile
  case class SentiWordNetFeedbackClass(count: String,synsetId:String,synsetPOS:String,swnPositivity:String,	swnNegativity:String,	feedbackPositivity:String,	feedbackNegativity:String,	date:String,	IPano:String,	IPcountry:String,	list:String)
  case class SentiWordNetFeedbackSpark(synsetPOS:String,swnPositivity:String,	swnNegativity:String,	feedbackPositivity:String, feedbackNegativity:String, Term:String, TermRank:String)
  // Question processing
  case class QuestionObj(sentence:String,sentenceWoSW:String,SentimentExtraction:Int,tokens:List[Token],PosSentence:String,var phaseTwoScore:Int)
  case class Token(index:String,word:String,posTag:String,lemma:String,var relationID:Int,var uris:List[String],var sentimentScore: Double)
  case class Sentence(sentence: String)
  // Phase two
  case class Expression(verb:String,noun:String,relationshipID:Int)
  // API Data
  case class noun(syn: List[String],ant: List[String])
  case class verb(syn: List[String],ant: List[String])
  case class Obj(noun:noun,verb:verb)