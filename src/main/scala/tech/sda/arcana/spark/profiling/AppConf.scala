package tech.sda.arcana.spark.profiling

/*
 * A class that contains the configuration of the Profiling App 
 */
object AppConf {
  //This to be entered by user
  val path = "/home/elievex/Repository/resources/"
  
  // MONGODB
  val host = "mongodb://127.0.0.1/"
  val dbName = "ArcanaDB"
  val firstPhaseCollection = "ArcanaTest"
  val secondPhaseCollection = "ArcanaPOS"
  val inputUri = "spark.mongodb.input.uri"
  val outputUri = "spark.mongodb.output.uri"
  // WordNet Dict
  val WordNetDict = path+"WordNet/3.0/dict"
  // SentiWord File
  val SentiWordFile=path+"sentiwordnet/SentiWordNet.txt"
  val SentiWordFilefeedback=path+"sentiwordnet/SentiWordNetFeedback.txt"
  
  // Malicious categories that we don't wish to answer
  var categories = List("military", "nuclear", "terrorism", "weapon", "technology", "security", "harm", "suicide", "war")
  // Malicious expression that we don't wish to answer
  var tuples = List(("kill", "person"),("bomb", "building"),("make","bomb"))
 
  
}