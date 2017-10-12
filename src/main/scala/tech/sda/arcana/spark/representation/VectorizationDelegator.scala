package tech.sda.arcana.spark.representation
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** A class to deal with the vectorization of the data
 *  @constructor create a new vector delegator with a spark context
 *  @param SparkContext object, which tells Spark how to access a cluster
 *  @param vectorLength represents the length of the delegated vector 
 */
class VectorizationDelegator(sc:SparkContext,vectorLength:Int) extends Serializable {
  val this.sc:SparkContext=sc
  val this.vectorLength:Int=vectorLength
    
     /** parse the vector representation regarding GloVe: Global Vectors for Word Representation
     *   @param line one line from the representation
     *   @return pair of words and vectors, each word with it's vector representation
     */
    def ParseVecGlov(line:String)={
    //The condition to check if we got data but it returns a problem 
    //in the data type it changes to RDD[Unit] when mapping so it is omitted
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
  
    def ParseVecWord2vec(line:String)={

  }
    
    def ParseVecWord2vec_f(line:String)={

  }
    
    def ParseVecAdagram(line:String)={

  }
    
   def ParseVecFasttext(line:String)={

  }
   
   def ParseVecWang2vec(line:String)={

  }
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

}