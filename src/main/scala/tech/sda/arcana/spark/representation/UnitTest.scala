package tech.sda.arcana.spark.representation
import java.io._
import org.apache.spark.rdd.RDD
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.nn.View
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.collection.mutable.ListBuffer
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.dataset.Sample
import com.intel.analytics.bigdl.dataset.MiniBatch
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.nn.ClassNLLCriterion
import com.intel.analytics.bigdl.nn.MSECriterion
import com.intel.analytics.bigdl.utils.T
import shapeless._0
import com.intel.analytics.bigdl.utils.Engine
import tech.sda.arcana.spark.neuralnetwork.model.LeNet5Model
import tech.sda.arcana.spark.neuralnetwork.model.DyLeNet5Model
import tech.sda.arcana.spark.neuralnetwork.model.AlexNetModel
import tech.sda.arcana.spark.neuralnetwork.model.GoogLeNetModel
import com.intel.analytics.bigdl.nn.Reshape
import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.visualization._

object sentenceToTensor {
  //Transfer one question one sentence to multi-represential tensorcom.intel.analytics.bigdl.visualization
  
   val vectorLength:Int=50
  val sentenceWordCount:Int=20
  
      //return each line of the glov representation as follows:
      // { String(word),Array[string](the vector representatiparsedQuestionson) }
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
      
      //return each question of the text file as an array of words with an mumber in the
      //beginning to identify the order of this question
        def parseQuestion(line:String)={
          val words=line.toLowerCase().split(" ")
          //build the return element which looks as follows:
          //RDD[word it self inside the sentence,(number of the sentence,number of the element)]
          val orderedWords=words.drop(1).zipWithIndex.map{case(line,i) => (line,(words(0).toLong,i))}
          (orderedWords)
        }
        //////////////////////////////************************************************
        def clean(sentence:String)={
          val cleanedSen=sentence.replaceAll("\\p{Punct}", " $0 ")
          (cleanedSen)
        }
        
         def calculateLongestWordsSeq(questions:RDD[String])={
          println(questions.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b))
        }
         
          def calculateQuestionsNumber(questions:RDD[String])={
          println(questions.count())
        }
        
          def testte(sentence:(Long, Iterable[((Long, Int), Array[String])]))={
            //if(sentence!=null){
            val tensor=Tensor[Float](1,sentenceWordCount,vectorLength)
            val tensorStorage= tensor.storage.fill(0, 1, sentenceWordCount*vectorLength-1)
            //the reverse here to make the word order upside down
            var vec=sentence._2.toSeq.sortBy(x=>x._1._2).reverse
            /*vec.foreach{x=>println(x._1)
                        println(x._2)}*/
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
        
            def sasa(tenso:Tensor[Float])={
          
            val label=Tensor[Float](T(1f))
            val sample=Sample(tenso,label)
            
        

            (sample)
        }

                  
          def Core(model:String):SparkContext={
         //initiate spark using the engine
         val conf = Engine.createSparkConf()
           .setAppName(model)
           .set("spark.task.maxFailures", "1")
           .setMaster("local[3]")
         val sc = new SparkContext(conf)
         Engine.init
         return sc
          }
  
    def main(args:Array[String]){
      

      
      
      
          // Set the log level to only print errorsval great=groupedResultTest.map(test)
          Logger.getLogger("org").setLevel(Level.ERROR)
          
          // Create a SparkContext using every core of the local machine
          // val sc = new SparkContext("local[*]", "MinTemperatures")
          val sc=Core("model")
          
         
          
          // Read each line of input data
          val lines = sc.textFile("/home/mhd/Desktop/ARCANA Resources/glove.6B/glove.6B.50d.txt")
          
          // Read the questions
          val questions = sc.textFile("/home/mhd/Desktop/Data Set/TestNowM.txt")

          //Give each question an Id or an order
          val orderedQuestions=questions.zipWithIndex().map{case(line,i) => i.toString+" "+line}
          val parsedLines = lines.map(parseLine)
          //Convert each question to array of words (the output Array[(String, (String, Int))])
          //val parsedQuestions= orderedQuestions.map(parseQuestion)
          //for the joining sake I used flat map to discard the array (the output (String, (String, Int)))
          val parsedQuestions=orderedQuestions.flatMap(parseQuestion)
          //parsedQuestions.foreach{x=>printf("\nString= %s Line= %s Word= %s",x._1,x._2._1,x._2._2) }
          //the result looks as follows:
          // RDD[(String, ((String        , Int)     ,       Array[String]))]
          // RDD[(word,   ((sentence order,word order,vector representation))]
          //for(i<-result)
          //      i._1,   ((i._2._1._1    ,i._2._1._2, i._2._2             ))
          val result= parsedQuestions.join(parsedLines)
          
          // try to simplify the structure 
          val resultTest= result.map{case(a,b)=>b}
          
          //(Long, Iterable[((Long, Int), Array[String])])
          val groupedResultTest=resultTest.groupBy(x=>x._1._1)
          
          val great=groupedResultTest.map(testte)
          //great.collect().foreach(println)
          //val answer=great.collect()    
          
          val sddf= great.map(sasa)
     
          val optimizer = Optimizer(
              model = DyLeNet5Model.build(20,50,5),
              sampleRDD = sddf,
              criterion = ClassNLLCriterion[Float](),
              batchSize = 3
            )
            println("reach here")
            
            
            //optimizer.setValidation(trigger, dataset, vMethods)
            //optimizer.setOptimMethod(method)
            //optimizer.setEndWhen(endWhen)
            /*
            optimizer
            .setValidation(
              trigger = Trigger.everyEpoch,
              dataset = validationSet,
              vMethods = Array(new Top1Accuracy))
            .setOptimMethod(new Adagrad(learningRate=0.01, learningRateDecay=0.0002))
            .setEndWhen(Trigger.maxEpoch(param.maxEpoch))
            .optimize()
            */
            /*
            val nowModel = LeNet5Model.graph(5)
            nowModel.saveGraphTopology("/home/mhd/Desktop/bigdl_summaries")
            
            val logdir = "/home/mhd/Desktop/bigdl_summaries"
            val appName = "testApp"
            val trainSummary = TrainSummary(logdir, appName)
            val validationSummary = ValidationSummary(logdir, appName)
            optimizer.setTrainSummary(trainSummary)
            optimizer.setValidationSummary(validationSummary)
            optimizer.setValidation(Trigger.everyEpoch ,sddf, Array(new Top1Accuracy),3)
            */
            val trained_model=optimizer.optimize()
            //val evaluateResult=trained_model.evaluate(sddf, Array(new Top1Accuracy), None)
            //val evaluateResult = trained_model.evaluate(testSet, Array(new Top1Accuracy), None)
            //evaluateResult.foreach(println)  
            
            
            
            val re=trained_model.predict(sddf).collect()
            re.foreach(println)
    
    }        
}