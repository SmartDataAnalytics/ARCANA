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
import com.intel.analytics.bigdl.nn.L1Cost
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
import com.intel.analytics.bigdl.optim._
import com.intel.analytics.bigdl.example.loadmodel.AlexNet
/////////////
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.utils.{T, Table}
import com.intel.analytics.bigdl.Module
import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.{Graph, _}
import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.nn.abstractnn.TensorCriterion


object sentenceToTensor {
  //Transfer one question one sentence to multi-represential tensorcom.intel.analytics.bigdl.visualization
  
   val vectorLength:Int=50
  val sentenceWordCount:Int=40

 
  
          def parseLine(line:String)={
                val fields = line.split(" ")
                val word = fields(0).toString()
                val representation:Array[String]=new Array[String](vectorLength)
                  for(x<-0 to vectorLength-1){
                    representation(x)=fields(x+1)
                  }
          (word, representation)
        }
   
             def labelTensors(line:String)={
                val fields = line.replaceAll("\\s", "").split(",")
                val id = fields(0).toLong
                val label=fields(1).toInt
          (id, label)
        }
             
         def wow(x:(Long, (Int, Tensor[Float])))={
          val in = "/home/mhd/Desktop/Investigate.txt"
          val writer = new PrintWriter(new File(in))
          
          writer.write("Sentence id=" + x._1.toString())
          writer.write("Class=" + x._2._1.toString())
          
            for(i <- 0 to 40){
              for(j <- 0 to 50){
                writer.write((x._2._2(i)(j)).toString()+",")
              }
              writer.write("\n")
            } 
          writer.close()
          }
      
        def parseQuestion(line:String)={
          val words=line.toLowerCase().split(" ")
          val orderedWords=words.drop(1).zipWithIndex.map{case(line,i) => (line,(words(0).toLong,i))}
          (orderedWords)
        }
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
            (sentence._1,tensor)

        }
       
            def sasa(inin:(Long, (Int, Tensor[Float])))={
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

        def sasa1(inin:(Long, (Int, Tensor[Float])))={
            var kind:Int =0
            var label:Tensor[Float] = Tensor[Float](T(T(-5f),T(-5f),T(-5f)))
            if(inin._2._1 == 1){
              label=Tensor[Float](T(T(2f),T(2f),T(2f)))
              kind=1
            }
            if(inin._2._1 == 0){
              label=Tensor[Float](T(T(2f),T(1f),T(1f)))
              kind=1
            }
            if(inin._2._1 == -1){
              label=Tensor[Float](T(T(1f),T(1f),T(1f)))
              kind=0
            }
            if(inin._2._1 == -2){
              label=Tensor[Float](T(T(2f),T(2f),T(2f)))
              kind=0
            }
            //println(label)
             val sample=Sample(inin._2._2,label)            
            (kind,sample)
        }
            
        def sasa2(inin:(Long, (Int, Tensor[Float])))={
            var kind:Int =0
            var label:Tensor[Float] = Tensor[Float](T(-5f,-5f,-5f))
            if(inin._2._1 == 1){
              label=Tensor[Float](T(2f,2f,2f))
              kind=1
            }
            if(inin._2._1 == 0){
              label=Tensor[Float](T(2f,1f,1f))
              kind=1
            }
            if(inin._2._1 == -1){
              label=Tensor[Float](T(1f,1f,1f))
              kind=0
            }
            if(inin._2._1 == -2){
              label=Tensor[Float](T(2f,2f,2f))
              kind=0
            }
            //println(label)
             val sample=Sample(inin._2._2,label)            
            (kind,sample)
        }
        
           def sasa3(inin:(Long, (Int, Tensor[Float])))={
               var kind:Int =0
                var label:Tensor[Float] = Tensor[Float](T(T(-5f),T(-5f),T(-5f)))
                if(inin._2._1 == 1){
                  label=Tensor[Float](T(T(2f,2f,2f),T(2f,2f,2f),T(2f,2f,2f)))
                  kind=1
                }
                if(inin._2._1 == 0){
                  label=Tensor[Float](T(T(2f,2f,2f),T(1f,2f,2f),T(1f,2f,2f)))
                  kind=1
                }
                if(inin._2._1 == -1){
                  label=Tensor[Float](T(T(1f,2f,2f),T(1f,2f,2f),T(1f,2f,2f)))
                  kind=0
                }
                if(inin._2._1 == -2){
                  label=Tensor[Float](T(T(2f,2f,2f),T(2f,2f,2f),T(2f,2f,2f)))
                  kind=0
                }
                //println(label)
                 val sample=Sample(inin._2._2,label)            
                (kind,sample)
        }
        
          def Core(model:String):SparkContext={
         val conf = Engine.createSparkConf()
           .setAppName(model)
           .set("spark.task.maxFailures", "1")
           .setMaster("local[1]")
         val sc = new SparkContext(conf)
         Engine.init
         return sc
          }

    def main(args:Array[String]){

          val mappingGen:Boolean = false
          
          if(mappingGen){
            val logdir = "/home/mhd/Desktop/Data_Set/Mapping.txt"
                    
              
             val writer = new PrintWriter(new File(logdir))
        
             for(i <- 0 to 1599)
               if(i<560){
                 writer.write(i+",0 \n")
               }
               else{
                 if(i<1120)
                   writer.write(i+",1 \n")
                   else
                     if((i<1360))
                      writer.write(i+",-1 \n")
                      else
                        writer.write(i+",-2 \n")
               }
              writer.close()
            }       
          
          
   
    else{
          Logger.getLogger("org").setLevel(Level.ERROR)

          val sc=Core("testApp2")

          val lines = sc.textFile("/home/mhd/Desktop/ARCANA Resources/glove.6B/glove.6B.50d.txt")

          val questions = sc.textFile("/home/mhd/Desktop/WorkSpace/ARCANA/ARCANA Questions/Test_Data_Set/TestNow.txt")

          val questionInitializer=new QuestionsInitializer(sparkContext=sc) 

          val orderedQuestionss=questions.zipWithIndex().map{case(line,i) => i.toString+" "+line}

          val orderedQuestions = orderedQuestionss.map(questionInitializer.clean)
          
          val parsedLines = lines.map(parseLine)

          val parsedQuestions=orderedQuestions.flatMap(parseQuestion)

          val result= parsedQuestions.join(parsedLines)
          
          val resultTest= result.map{case(a,b)=>b}
          
          val groupedResultTest=resultTest.groupBy(x=>x._1._1)
          
          val great=groupedResultTest.map(testte)
    
          
          val mappings = sc.textFile("/home/mhd/Desktop/WorkSpace/ARCANA/ARCANA Questions/Test_Data_Set/Mapping.txt")

          val maps=mappings.map(labelTensors)
          
          val alla=maps.join(great)
          
          val sddf= alla.map(sasa)   
          
          val trainSamples=sddf.filter(x=>x._1==1).map{case(a,b)=>b}
          val testSamples=sddf.filter(x=>x._1==0).map{case(a,b)=>b}

          val optimMethod1 = new SGD[Float](learningRate= 0.1,learningRateDecay=0.0002)
          
          val optimizer = Optimizer(
              //model = DyLeNet5Model.build(40, 50, 2),
              model = AlexNetModel.build(40, 50, 2),
              //1model=GoogLeNetModel.build(40, 50, 2),
              //model=GoogLeNetModel.build_no(40, 50, 2),
              sampleRDD = trainSamples,
              criterion = ClassNLLCriterion[Float](),
              batchSize = 8
            )
            optimizer.setOptimMethod(optimMethod1)
            println("reach here")
            
   
            val logdir = "/home/mhd/Desktop/bigdl_summaries"
            val appName = "AlexUni"
            val trainSummary = TrainSummary(logdir, appName)
            val validationSummary = ValidationSummary(logdir, appName)
            optimizer.setTrainSummary(trainSummary)
            optimizer.setValidationSummary(validationSummary)
            optimizer.setValidation(Trigger.everyEpoch ,testSamples, Array(new Top1Accuracy),3)
            //optimizer.setCheckpoint(logdir+"/"+appName, Trigger.everyEpoch)
            //val trainLoss = trainSummary.readScalar("Loss")
            //val validationLoss = validationSummary.readScalar("Loss")
            //optimizer.setEndWhen(Trigger.minLoss(0.3f))
            optimizer.setEndWhen(Trigger.maxEpoch(2))
            println("reach here")
            val trained_model=optimizer.optimize()
            println("end here")
            //val evaluateResult=trained_model.evaluate(sddf, Array(new Top1Accuracy), None)
            //val evaluateResult = trained_model.evaluate(testSet, Array(new Top1Accuracy), None)
            //evaluateResult.foreach(println)  

            println("--------------------------")
            //trained_model.predict(testSamples, 12, true).foreach(println)
            println("--------------------------")
            val re= trained_model.evaluate(testSamples, Array(new Top1Accuracy), None)
            re.foreach(println)
      }
  
    }
  }
