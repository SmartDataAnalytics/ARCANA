package tech.sda.arcana.spark.profiling
/*
 * A class that defines the malicious Categories that should be dealt with 
 */
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

