package tech.sda.arcana.spark.representation
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import com.intel.analytics.bigdl.tensor.Tensor

/** A class with many transformation functionalities between structures and tensors
 *  @constructor create a new transformer with a spark context, longestWordsSeq, vectorLength, questionsNumber
 *  @param SparkContext object, which tells Spark how to access a cluster 
 *  @param longestWordsSeq the number of words in the longest sentence in the text 
 *  @param vectorLength the length of the vector representation
 *  @param questionsNumber the number of the questions in the text
 */ 
class QuestionTensorTransformer(sc:SparkContext,longestWordsSeq: Int,vectorLength: Int,questionsNumber: Int= -1) {
  val this.sc:SparkContext=sc
  val this.longestWordsSeq:Int=longestWordsSeq
  val this.vectorLength:Int=vectorLength
  val this.questionsNumber:Int=questionsNumber
  
  /** Transform the following structure (Long, Iterable[((Long, Int), Array[String])])
   *  to a tensor
   *  @param sentence a structure as follows (Long, Iterable[((Long, Int), Array[String])])
   *  @return Tensor
   */
  def transform(sentence:(Long, Iterable[((Long, Int), Array[String])]))={
      //if(sentence!=null){
      val tensor=Tensor[Float](longestWordsSeq,vectorLength)
      val tensorStorage= tensor.storage.fill(0, 1, longestWordsSeq*vectorLength-1)
      //the reverse here to make the word order upside down
      var vec=sentence._2.toSeq.sortBy(x=>x._1._2).reverse
      var storageCounter:Int=0
      while(vec.lastOption.exists(p=>true) == true){
      //while(storageCounter<sentenceWordCount*vectorLength-1){
      vec.last._2.foreach{x=>
                          //printf("\n Counter= %d",storageCounter)
                          tensorStorage(storageCounter)=x.toFloat
                          storageCounter=storageCounter+1
                          }
      vec=vec.init
      
      }
      (tensor)
      //}
  }
        
}