package tech.sda.arcana.spark.profiling
import scala.collection.mutable.ListBuffer

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
  case class DBRecord(_id: Int, uri: String, expression: String, category: String, score: Double, weight: Double, objectOf: String) // score = cosine similary - weight = sentiment analysis
  case class SentiWordNetClass(POS:String, ID:String, PosScore:String, NegScore:String, SynsetTerms:String)
  case class SentiWordSpark(POS:String, ID:String, PosScore:String, NegScore:String, Term:String,TermRank:String)