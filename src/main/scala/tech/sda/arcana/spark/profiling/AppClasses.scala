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
  //var categories = Array("military", "nuclear", "terrorism", "weapon", "technology", "security", "harm", "suicide")
}

  case class Synonym(word: String, similarity: Double)
  case class DBRecord(_id: Int, uri: String, expression: String, category: String, score: Double, weight: Double, objectOf: String) // score = cosine similary - weight = sentiment analysis
