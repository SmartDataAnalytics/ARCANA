package tech.sda.arcana.spark.profiling

/*
 * A class that contains the configuration of the Profiling App 
 */
object AppConf {
  // MONGODB
  val host = "mongodb://127.0.0.1/"
  val dbName = "ArcanaDB"
  val defaultCollection = "ArcanaTest"
  // WordNet Dict
  val WordNetDict = "src/WordNet/3.0/dict"
  // SentiWord File
  val SentiWordFile="/home/elievex/Repository/ExtResources/SentiWordNet/home/swn/www/admin/dump/SentiWordNet.txt"
  
  // CATEGORIES
  var categories = List("military", "nuclear", "terrorism", "weapon", "technology", "security", "harm", "suicide", "war")
}