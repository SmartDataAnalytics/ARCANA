package tech.sda.arcana.spark.representation
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.utils.T

/**A class with many transformation functionalities between tensors and samples
 * @constructor create a new transformer with a spark context
 * @param SparkContext object, which tells Spark how to access a cluster 
 */
class TensorSampleTransformer(sparkContext: SparkContext) extends Serializable {
    
    /** Initialize the samples by labeling the input tensor with positive label
     *  @param tensor which represent the question
     *  @return clean question
     */
    def initializePositiveSample(tensor:Tensor[Float])={
            val label=Tensor[Float](T(T(1f),T(0f)))
            val sample=Sample(tensor,label)
            (sample)
    }
    
    /** Initialize the samples by labeling the input tensor with negative label
     *  @param tensor which represent the question
     *  @return clean question
     */
    def initializeNegativeSample(tensor:Tensor[Float])={
            val label=Tensor[Float](T(T(0f),T(1f)))
            //try it like this View().forward(Tensor[Float](T(T(0f),T(1f))))
            //and see if there are differences
            //and try nother is a 1-element tensor representing its category
            //it stated in the documentation
            val sample=Sample(tensor,label)
            (sample)
    }
    
    /** Initialize the samples by labeling the input tensors with negative 
     *  and positive labels with indicating the train and the test samples
     *  -1,-2 indicates the two classes for the test tensors
     *   1,2 indicates two classes for the train tensors 
     *   @param a tensor with Long(question id) and (Int 1,0 training samples)
     *   (-1,-2 for testing samples)
     *   @return an Int (1 for training samples and 0 for testing samples)
     *   with the samples (labeled tensors)
     */
    def initializeAllSamples(inin:(Long, (Int, Tensor[Float])))={
            var kind:Int =0
            var label:Tensor[Float] = Tensor[Float](T(-5f))
            if(inin._2._1 == 1){
              label=Tensor[Float](T(2f))
              kind=1
            }
            if(inin._2._1 == 0){
              label=Tensor[Float](T(1f))
              kind=1
            }
            if(inin._2._1 == -1){
              label=Tensor[Float](T(1f))
              kind=0
            }
            if(inin._2._1 == -2){
              label=Tensor[Float](T(2f))
              kind=0
            }
            //println(label)
             val sample=Sample(inin._2._2,label)            
            (kind,sample)
        }
    
    
    /** Merge two samples together
     *  @param fData first [RDD of tensors] you want to merge
     *  @param sData second [RDD of tensors] you want to merge
     *  @return merged [RDD contains all the input tensors]
     */
    def mergeSamples(fData:RDD[Tensor[Float]],sData:RDD[Tensor[Float]])={
          val merged=fData.union(sData)
          (merged)
    }
    
    /** Initialize the mappings structure needed to map the tenors
     *  @param line the mappings line
     *  @return the each question id and its real classification 
     */ 
     def mappingInit(line:String)={
      val fields = line.replaceAll("\\s", "").split(",")
      val id = fields(0).toLong
      val label=fields(1).toInt
      (id, label)
    }
    
    def initializeCustomSample()={

    }
                      
  
}