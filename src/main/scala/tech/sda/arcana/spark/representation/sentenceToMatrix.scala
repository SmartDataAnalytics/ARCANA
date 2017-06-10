package tech.sda.arcana.spark.representation
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j._


object sentenceToMatrix {
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
            val sc = new SparkContext("local[*]", "MinTemperatures")
            
            // Read each line of input data
            val lines = sc.textFile("/home/mhd/Desktop/ARCANA Resources/glove.6B/glove.6B.50d.txt")
            
            val parsedLines = lines.map(parseLine)
            
            //val representation = parsedLines.filter( (x) => (x._1 == "the") )
            //WARNING check if those vectors are in the same order of the words in the sentence
            val representation = parsedLines.filter( (x) => (sentense.contains(x._1)) )
            
            val vectors=representation.map(x => x._2)
            
            val result = vectors.collect()
            
            //sentence representation
            val senRep:Array[Array[String]]=Array.ofDim[String](sentenceWordCount,vectorLength)
              
            for(x <- 0 to sentenceWordCount-1)
              senRep(x)=result(x)
                

    }
}