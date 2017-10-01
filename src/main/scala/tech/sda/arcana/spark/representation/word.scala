package tech.sda.arcana.spark.representation
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.collection.mutable.ListBuffer
import tech.sda.arcana.spark.classification.cnn.Core

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
  val vectorLength:Int=50
  val sentenceWordCount:Int=4
  
  //return each line of the glov representation as follows:
  // { String(word),Array[string](the vector representation) }
      def parseLine(line:String)={
      //if(!line.isEmpty()){
            val fields = line.split(" ")
            val word = fields(0).toString()
            val representation:Array[String]=new Array[String](vectorLength)
              for(x<-0 to vectorLength-1){
                representation(x)=fields(x+1)
              }
     // }
      (word, representation)
    }

  
    def main(args:Array[String]){
      
            
            val sentense=Array("my","name","is","ghost")
           
            // Set the log level to only print errors
            Logger.getLogger("org").setLevel(Level.ERROR)
            
            // Create a SparkContext using every core of the local machine
           // val sc = new SparkContext("local[*]", "MinTemperatures")
            val s=new Core("model","train","label","test")
            val sc=s.initialize()
            
            
            // Read each line of input data
            val lines = sc.textFile("/home/mhd/Desktop/ARCANA Resources/glove.6B/glove.6B.50d.txt")
            
            val parsedLines = lines.map(parseLine)
            
            //val representation = parsedLines.filter( (x) => (x._1 == "the") )
            //WARNING check if those vectors are in the same order of the words in the sentence
            val representation = parsedLines.filter( (x) => (sentense.contains(x._1)) )
            //gather the answers and continue arranging without Spark   
            val result = representation.collect()
            
            //this section has been done locally because the data is rather small
            val senRep:ListBuffer[Array[String]]=ListBuffer()
            for(i <- 0 to sentenceWordCount-1)
              for(j<-result)
                if(j._1 == sentense(i))
                  senRep += j._2
            
            //achtung this is done locally 
            for(i<-senRep){
              for(j<-i){
                print(j)
                print(" ")
              }
              println()
            }
            //for 2d conversion
            //matrix.grouped(3).toArra
            
            
            
            //val vectors=representation.map(x => x._2)
            
            /*
            //sentence representation
            //val senRep:Array[Array[String]]=Array.ofDim[String](sentenceWordCount,vectorLength)
              
            for(x <- 0 to sentenceWordCount-1)
              senRep(x)=result(x)
              
           
            //for the sake of testing
            for(x<- 0 to sentenceWordCount-1){
              for(j<-senRep(x)){
                print(j)
                print(" ")
              }
              println("------------------")
            }
          */
            
             
                

    }
}
