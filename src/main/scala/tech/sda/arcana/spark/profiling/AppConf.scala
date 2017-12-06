package tech.sda.arcana.spark.profiling

/*
 * A class that contains the configuration of the Profiling App 
 */
object AppConf {
  //This to be entered by user
  //val path = "/home/elievex/Repository/resources/"
  
  // MONGODB
  val host = "mongodb://127.0.0.1/"
  val dbName = "ArcanaDB"
  val firstPhaseCollection = "ArcanaCAT"
  val secondPhaseCollection = "ArcanaPOS"
  val inputUri = "spark.mongodb.input.uri"
  val outputUri = "spark.mongodb.output.uri"
  // Word2Vec
  val VectorSize=10
  val MinCount=0
  
  // dbpedia
  val dbpedia = "DBpedia/*"
  // Word2Vec
  val CategoryData = "Word2Vec/Data/CategoryData"
  val DatasetData = "Word2Vec/Data/DatasetData"
  val Word2VecModel = "Word2Vec/Model/Word2VecModel"
  // WordNet Dict
  val WordNetDict = "WordNet/3.0/dict"
  // SentiWord File
  val SentiWordFile = "SentiWordNet/SentiWordNet.txt"
  val ProcessedSentiWordFile = "SentiWordNet/processed/file.csv"
  val SentiWordFilefeedback = "SentiWordNet/SentiWordNetFeedback.txt"
  // Questions
  val Questions = "Questions"
  
  // Malicious categories that we don't wish to answer
  var categories = List("Tr","military", "nuclear", "terrorism", "weapon", "technology", "security", "harm", "suicide", "war")
  // Malicious expression that we don't wish to answer
  var tuples = List(("kill", "person"),("bomb", "building"),("make","bomb"))
 
  
}