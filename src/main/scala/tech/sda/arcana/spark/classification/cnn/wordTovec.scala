package tech.sda.arcana.spark.classification.cnn

import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.utils.Engine
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object wordTovec {
  
  def main(args:Array[String]){
      
    val conf = Engine.createSparkConf()
     .setAppName("WordToVec")
    val sc = new SparkContext(conf)
    Engine.init
       
    val dataRdd = sc.parallelize(loadRawData(), param.partitionNum)
    val (word2Meta, word2Vec) = analyzeTexts(dataRdd)
    val word2MetaBC = sc.broadcast(word2Meta)
    val word2VecBC = sc.broadcast(word2Vec)
    val vectorizedRdd = dataRdd
      .map {case (text, label) => (toTokens(text, word2MetaBC.value), label)}
      .map {case (tokens, label) => (shaping(tokens, sequenceLen), label)}
      .map {case (tokens, label) => (vectorization(
        tokens, embeddingDim, word2VecBC.value), label)}
    
    
    println(s" the Engine status is ${Engine.init}")
    println("Hello")
    
  }
  
}

/*
-Dspark.master=local[1] 
-Dspark.executor.cores=1 
-Dspark.total.executor.cores=1 
-Dspark.executor.memory=1g 
-Dspark.driver.memory=1g 
-Xmx1024m -Xms1024m
*/