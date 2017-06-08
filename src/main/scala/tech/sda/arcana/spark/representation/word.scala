package tech.sda.arcana.spark.representation
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object word {
  //The user going to choose the approach from those:
  //Word2vec
  //Glove
  //word2vec-f
  //Adagram
  //Fasttext
  //Wang2vec
  //other representations https://www.quora.com/Could-you-please-explain-the-choice-constraints-of-the-pros-cons-while-choosing-Word2Vec-GloVe-or-any-other-thought-vectors-you-have-used
  //The user going to choose the length of vector of each word with respect 
  //to the previous approach
      def parseLine(line:String)= {
            val fields = line.split(" ")
            println(fields)
            val word = fields(0).toString()
        //50 has to be dynamic in the future
            val representation:Array[String]=new Array[String](50)
        //10 is only a test
              for(x<-0 to 10){
                representation(x)=fields(x+1)
              }
            (word, representation)
    }
  
       def main(args: Array[String]) {
     
            // Set the log level to only print errors
            Logger.getLogger("org").setLevel(Level.ERROR)
    
            // Create a SparkContext using every core of the local machine
            val sc = new SparkContext("local[*]", "WordToVec")
            
            // The path of the vector representation
            val lines = sc.textFile("PATH")
            
            val parsedLines = lines.map(parseLine)
            //the word shoud be in the constructer of the calss in the future
            val representation = parsedLines.filter(x => x._1 == "WORD")
            
            val result = representation.collect()
            
            println(result)
     }
}
