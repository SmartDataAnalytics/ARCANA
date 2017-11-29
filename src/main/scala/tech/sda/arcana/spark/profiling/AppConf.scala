package tech.sda.arcana.spark.profiling

/*
 * A class that contains the configuration of the Profiling App 
 */
object AppConf {
  // MONGODB
  val host = "mongodb://127.0.0.1/"
  val dbName = "ArcanaDB"
  val firstPhaseCollection = "ArcanaTest"
  val secondPhaseCollection = "ArcanaPOS"
  // WordNet Dict
  val WordNetDict = "src/WordNet/3.0/dict"
  // SentiWord File
  val SentiWordFile="src/main/resources/SentiWordNet/SentiWordNet.txt"
  val SentiWordFilefeedback="src/main/resources/SentiWordNet/SWN_feedback_20130513.txt"
  
  // Malicious categories that we don't wish to answer
  var categories = List("military", "nuclear", "terrorism", "weapon", "technology", "security", "harm", "suicide", "war")
  // Malicious expression that we don't wish to answer
  var tuples = List(("kill", "person"),("bomb", "building"),("make","bomb"))
 
}