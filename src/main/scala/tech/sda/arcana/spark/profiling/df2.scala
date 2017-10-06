
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
/*
 * An Object that have some Scala Concepts that needs to be visited time to time 
 */
object LearningScala2 {
  val sc = new SparkContext("local[*]", "RatingsCounter")
  val lines = sc.textFile("../ml-100k/u.data")
  
  // let's say we have 4 column, split them and grab the third one 
  val ratings = lines.map(x => x.toString().split("\t")(2))
  //How many times it occured 
  val results = ratings.countByValue()
  //convert to seq and sort by first field
  val sortedResults = results.toSeq.sortBy(_._1)
  
  /////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  def parseLine(line: String) = {
      val fields = line.split(",")
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      // return key value pair
      (age, numFriends)
  }
  val lines2 = sc.textFile("../fakefriends.csv")
  val rdd = lines2.map(parseLine)
  val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
  val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
  val results2 = averagesByAge.collect()
  
  /////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////
  
   def parseLine2(line:String)= {
  val fields = line.split(",")
  val stationID = fields(0)
  val entryType = fields(2)
  val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
  (stationID, entryType, temperature)
  }
  val lines3 = sc.textFile("../1800.csv")
  val parsedLines3 = lines3.map(parseLine2)
  
  val minTemps = parsedLines3.filter(x => x._2 == "TMIN")
  val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
  
  //val minTempsByStation = stationTemps.reduceByKey( (x,y) => min(x,y))
  //val results3 = minTempsByStation.collect()
  /////////////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  //Add your column
  import org.apache.spark.sql.functions.udf
 // val square = (x => x*x)
 // squaredDF = df.withColumn("square", square('value'))
  
  

}