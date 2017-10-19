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
class TensorSampleTransformer(sc: SparkContext) {
    
    /** Initialize the samples by labeling the input tensor with positive label
     *  @param tensor which represent the question
     *  @return clean question
     */
    def initializePositiveSample(tensor:Tensor[Float])={
            val label=Tensor[Float](T(1f))
            val sample=Sample(tensor,label)
            (sample)
    }
    
    /** Initialize the samples by labeling the input tensor with negative label
     *  @param tensor which represent the question
     *  @return clean question
     */
    def initializeNegativeSample(tensor:Tensor[Float])={
            val label=Tensor[Float](T(-1f))
            val sample=Sample(tensor,label)
            (sample)
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
                 
    def initializeCustomSample()={

    }
                      
  
}