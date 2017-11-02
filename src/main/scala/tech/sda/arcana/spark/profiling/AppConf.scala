package tech.sda.arcana.spark.profiling

/*
 * A class that contains the static infromation that the Database will be using 
 */
object AppConf {
  
  // MONGODB
  val host = "mongodb://127.0.0.1/"
  val dbName = "ArcanaDB"
  val defaultCollection = "ArcanaTest"
  // CATEGORIES
  var categories = List("military", "nuclear", "terrorism", "weapon", "technology", "security", "harm", "suicide", "war")
}