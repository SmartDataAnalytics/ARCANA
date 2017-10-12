package tech.sda.arcana.spark.representation
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import shapeless._0

/** A class contains all the important functionality needed to initialize a bunch of questions
 *  @constructor create a new questions initializer with a spark context
 *  @param SparkContext object, which tells Spark how to access a cluster 
 */
class QuestionsInitializer(sc:SparkContext) extends Serializable {
  var longestWordsSeq:Int=0
  var questionsNumber:Long=0
  val this.sc:SparkContext=sc

    /** Clean a bunch of questions by adding spaces between punctuations and other chars
    *   @param a question
    *   @return clean question
    */
     def clean(question:String):String={
          val cleanedSen=question.replaceAll("\\p{Punct}", " $0 ")
          (cleanedSen)
    }
  
    /** Calculate the number of questions exists in a file or any other structure
    *   @param bunch of questions (RDD of questions)
    *   @return the number of questions
    */
     def calculateQuestionsNumber(questions:RDD[String]):Long={
				questionsNumber=questions.count()
				(questionsNumber)
    }
     
    /** Calculate the longest question which contains the longest number of words
    *   @param bunch of questions (RDD of questions))
    *   @return the number of words for the longest question
    */
     def calculateLongestWordsSeq(questions:RDD[String]):Int={
       longestWordsSeq=questions.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
       (longestWordsSeq)
    }
      
    /** Add identifiers to each word in each sentence
    *   @param a question
    *   @return the sentence with different structure as follows
    *   (word it self inside the sentence,(number of the sentence,number of the element))
    */
      def parseQuestion(questions:String):Array[(String, (Long, Int))]={
        //All the representations available in Glov are lowercase
        val words=questions.toLowerCase().split(" ")
        //build the return element which looks as follows:
        //RDD[word it self inside the sentence,(number of the sentence,number of the element)]
        val orderedWords=words.drop(1).zipWithIndex.map{case(line,i) => (line,(words(0).toLong,i))}
        (orderedWords)
    }
}