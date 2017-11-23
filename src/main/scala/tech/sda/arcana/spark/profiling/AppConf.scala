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
  val SentiWordFile="src/main/resources/SentiWordNet/SentiWordNet.txt"
  val SentiWordFilefeedback="src/main/resources/SentiWordNet/SWN_feedback_20130513.txt"
  
  // CATEGORIES
  var categories = List("military", "nuclear", "terrorism", "weapon", "technology", "security", "harm", "suicide", "war")
}