package tech.sda.arcana.spark.profiling

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import java.io._
import org.apache.spark.rdd.RDD
/*
 * An Object that process the RDF Data and converts it to a format that is suitable to the Word2Vec algorithm
 */
object Dataset2Vec {
      val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Dataset2Vec")
      .getOrCreate()
      val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
      import sqlContext.implicits._
  /////////////////////////////////// PRINTING ///////////////////////////////////
  def showCategories(){
      AppConf.categories.foreach(line => println(line))//println(categories(1))
  }
  def showCategoryObjects(Categories: List[Category]){
    for (instance <- Categories){
      println("--"+instance.Category)//instance.uri.foreach(line => println(line.Uri))
    }
  }
  def showFirstTraverse(Categories: List[Category]){
    for (instance <- Categories){
      println("--"+instance.Category)//instance.uri.foreach(line => println(line.Uri))
       for (line <- instance.uri){
         println("--------"+line.Uri)
         for (x <- line.URIslist){
           println("----------------"+x.Uri)
         }
       }
    }
  } 
  def showSecondTraverse(Categories: List[Category]){
    for (instance <- Categories){
      println("--"+instance.Category)
       for (line <- instance.uri){
         println("--------"+line.Uri)
         for (x <- line.URIslist){
           println("----------------"+x.Uri)
           for (y <- x.URIslist){
             println("----------------------"+y.Uri)
           }
         }
       }
    }
  } 
  def showThirdTraverse(Categories: List[Category]){
    for (instance <- Categories){
      println("--"+instance.Category)
       for (line <- instance.uri){
         println("--------"+line.Uri)
         for (x <- line.URIslist){
           println("----------------"+x.Uri)
           for (y <- x.URIslist){
             println("----------------------"+y.Uri)
             for (z <- y.URIslist){
             println("--------------------------------"+z.Uri)
             }
           }
         }
       }
    }
  } 
  def showPreparedCategoryData(thirdTR: List[Category]){
      for(x<-thirdTR){
        for(y<-x.uri){
          println(y.FormedURI)
        }
      }
  }   
    def showPreparedDatasetData(thirdTR: List[Category]){
      for(x<-thirdTR){
        
          println(x.FormedURI)
       
      }
  }   
  ////////////////////////////////////////////////////////////////////////////////
  def rdfSubjectsToList(file: String): List[String]={
    val R=RDFApp.importingData(file)
    var subjectsList = R.select("Subject").rdd.map(r => r(0).asInstanceOf[String]).collect()
    subjectsList.toList.distinct
  }
  
  def fetchSubjectsRelatedToObjectWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Subject from triples where Object like '%$word%'") //> RLIKE for regular expressions
      return Res
  }
  
  def fetchObjectsRelatedToSubjectWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Object from triples where Subject like '%$word%'") 
      return Res
  }
  
  def fetchSubjectsRelatedToWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Subject from triples where Subject like '%$word%'") 
      return Res
  }
  
  def fetchObjectsRelatedToWord(DF: DataFrame, word: String): DataFrame={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Object from triples where Object like '%$word%'") 
      return Res
  }
  
  def fetchObjectsOfSubject(DF: DataFrame, word: String):List[RDFURI]={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT Object from triples where Subject = '$word'") 
      val UriList=Res.select("Object").rdd.map(r => r(0)).collect()
      UriList.toList.distinct.map(x => new RDFURI(x.asInstanceOf[String]))
  }
  
  def fetchAllOfWordAsSubject(DF: DataFrame, word: String):List[RDFURI]={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT * from triples where Subject like '%$word%'") 
      val UriList=Res.select("Subject").rdd.map(r => r(0)).collect()
      UriList.toList.distinct.map(x => new RDFURI(x.asInstanceOf[String]))
  }
  // First Traverse of the RDF Graph
  def firstTraverse(x:Category,DF: DataFrame):Category={
    x.uri.map(x=>(x.URIslist=fetchObjectsOfSubject(DF,x.Uri)))
    x
  }
  // Second Traverse of the RDF Graph
  def secondTraverse(xl: Category,DF: DataFrame):Category={
      for (fTR <- xl.uri){
        fTR.URIslist.map(x=>(x.URIslist=fetchObjectsOfSubject(DF,x.Uri)))
      }
    xl
  }
  // Third Traverse of the RDF Graph
  def thirdTraverse(xl: Category,DF: DataFrame):Category={
      for (fTR <- xl.uri){
        for (sTR <- fTR.URIslist){
           sTR.URIslist.map(x=>(x.URIslist=fetchObjectsOfSubject(DF,x.Uri))) 
        }
      }
    xl
  }

  // Get the data into a form that word2vec would operate on while obtaining it from categories
  def prepareCategoryData(data:List[Category]){
    //| Loop Categories
    for (instance <- data){
      //| Loop URIS of each category <Traverse 1>
       for (line <- instance.uri){
         line.FormedURI=line.Uri+" "+line.URIslist.map(_.Uri).mkString(" ")
         println(line.FormedURI)
         //| Loop URIS of each URI of category <Traverse 2>
         for (x <- line.URIslist){
           line.FormedURI +=" "+x.URIslist.map(_.Uri).mkString(" ")
         }
         //| Loop URIS of each URI of URI of category <Traverse 3>
         for(r<-line.URIslist){
            for (z <- r.URIslist){
             line.FormedURI +=" "+z.URIslist.map(_.Uri).mkString(" ")
             }
         }
         //This condition is to remove single URIs 
         if(line.FormedURI.count(_ == '>')>1){
           // replace double spaces with single spaces
           line.FormedURI=line.FormedURI.replaceAll(" +"," ")
           // remove trailing spaces
           line.FormedURI=line.FormedURI.replaceAll("""(?m)\s+$""", "")
         }
         else{
           line.FormedURI=""
         }
       }
    }
  }
    // Get the data into a form that word2vec would operate on while obtaining it from every subject
  def prepareDatasetData(data:List[Category]){
    //| Loop Categories
    for (instance <- data){

      instance.FormedURI=instance.Category+" "+instance.uri.map(_.Uri).mkString(" ")

       for (line <- instance.uri){
        instance.FormedURI+=" "+line.URIslist.map(_.Uri).mkString(" ")
       }

         for(r<-instance.uri){
            for (z <- r.URIslist){
             instance.FormedURI +=" "+z.URIslist.map(_.Uri).mkString(" ")
             }
         }

         for(r<-instance.uri){
            for (z <- r.URIslist){
             for (q <- z.URIslist){
               instance.FormedURI +=" "+q.URIslist.map(_.Uri).mkString(" ")
              }
            }
         }
     
         //This condition is to remove single URIs 
         if(instance.FormedURI.count(_ == '>')>1){
           // replace double spaces with single spaces
           instance.FormedURI=instance.FormedURI.replaceAll(" +"," ")
           // remove trailing spaces
           instance.FormedURI=instance.FormedURI.replaceAll("""(?m)\s+$""", "")
         }
         else{
           instance.FormedURI=""
         }
    }
  }
  // Fill the Category data into an RDD that is ready to be written 
  def prepareCategoryDataToRDD(thirdTR: List[Category]):RDD[String]={
      val sc = spark.sparkContext
      var myRDD=sc.emptyRDD[String]
      for(x<-thirdTR){
        for(y<-x.uri){
          myRDD++=sc.parallelize(Seq(y.FormedURI))
        }
      }
      myRDD.filter(_.nonEmpty)
  }    
    // Fill the Dataset data into an RDD that is ready to be written 
  def prepareDatasetDataToRDD(thirdTR: List[Category]):RDD[String]={
      val sc = spark.sparkContext
      var myRDD=sc.emptyRDD[String]
      for(x<-thirdTR){
 
          myRDD++=sc.parallelize(Seq(x.FormedURI))

      }
      myRDD.filter(_.nonEmpty)
  }  
  // Append data to the RDD when desired 
  def appendToRDD(data: String) {
     val sc = spark.sparkContext
     val rdd = sc.textFile("Word2VecData")  
     val extraRDD=sc.parallelize(Seq(data))
     val newRdd = rdd ++ extraRDD
     //newRdd.map(_.toString).toDF.show()
     newRdd.map(_.toString).toDF.coalesce(1).write.format("text").mode("append").save("Word2VecData")
     //newRdd.map(_.toString).toDF.coalesce(1).write.format("text").mode("overwrite").save("Word2VecData")
  }
 // This function reads the data and make the word2vecready data while working on subjects related to categories only
  def ceatingWord2VecCategoryData(DS: Dataset[Triple]){
      val R=DS //"src/main/resources/rdf2.nt"
      
      //| Fetch Categories
      var Categories = AppConf.categories
     
      //  var Categories = List("war","nuclear","Hunebed", "Paddestoel", "Buswachten")
      
      //| Converting each category to a Category Object with the list of URIs belonging to it
      var categoryOBJs=Categories.map(x => new Category(x,fetchAllOfWordAsSubject(R.toDF(),x)))
      
      //| Fetch the objects related to the URIs of each category
      var firstTR=categoryOBJs.map(x => firstTraverse(x,R.toDF()))

      var secondTR=firstTR.map(x => secondTraverse(x,R.toDF()))

      var thirdTR=secondTR.map(x => thirdTraverse(x,R.toDF()))
      // showThirdTraverse(thirdTR)

      prepareCategoryData(thirdTR)
      //showPreparedData(thirdTR)
      // Thread.sleep(5000)
      var myRDD=prepareCategoryDataToRDD(thirdTR)
      //myRDD.map(_.toString).toDF.show(false)     
 		
      myRDD.map(_.toString).toDF.coalesce(1).write.format("text").mode("overwrite").save("Word2VecCategoryData") // 'overwrite', 'append', 'ignore', 'error'.
  }
  
  // This function reads the data and make the word2vec ready data while working on each subject of the dataset
  def ceatingWord2VecDatasetData(DS: Dataset[Triple]){
      val R=DS

      var CategoriesNT=rdfSubjectsToList("src/main/resources/rdf2.nt")
      
      //| Converting each category to a Category Object with the list of URIs belonging to it
      var categoryOBJs=CategoriesNT.map(x => new Category(x,fetchObjectsOfSubject(R.toDF(),x)))

      //| Fetch the objects related to the URIs of each category
      var firstTR=categoryOBJs.map(x => firstTraverse(x,R.toDF()))

      var secondTR=firstTR.map(x => secondTraverse(x,R.toDF()))
      
      var thirdTR=secondTR.map(x => thirdTraverse(x,R.toDF()))
      //showThirdTraverse(thirdTR)
      
      prepareDatasetData(thirdTR)
      // showPreparedDatasetData(thirdTR)

      var myRDD=prepareDatasetDataToRDD(thirdTR)
      // myRDD.map(_.toString).toDF.show(100,false)     
 		
      myRDD.map(_.toString).toDF.coalesce(1).write.format("text").mode("overwrite").save("Word2VecDatasetData")
  }
  
    def MakeWord2VecModel(DS: Dataset[Triple]){
      
      //DS.show(false)
      
      //| Creates Word2Vec Data from Categories
      //> ceatingWord2VecCategoryData(DS)
      
      //| Creates Word2Vec Data from Dataset
      //> ceatingWord2VecDatasetData(DS)
  }
  
  def main(args: Array[String]) {
    val sc = spark.sparkContext

    println("~Stopping Session~")
    spark.stop()
  }
}


      //output to one file
      //finalRDD.coalesce(1, true).saveAsTextFile("testMie")
      //finalRDD.saveAsTextFile("out\\int\\tezt")
 
      //finalRDD.map(_.toString).toDF.write.mode("append").text("testMie")



