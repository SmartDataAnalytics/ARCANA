package tech.sda.arcana.spark.profiling

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import java.io._

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
      Categories.categories.foreach(line => println(line))//println(categories(1))
  }
  def showCategoryObjects(Categories: List[Category]){
    for (categoryN <- Categories){
        println(categoryN.Category)
        categoryN.uri.foreach(line => println(line.Uri))
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
  ////////////////////////////////////////////////////////////////////////////////
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
      //printList(UriList)
      UriList.toList.distinct.map(x => new RDFURI(x.asInstanceOf[String]))
  }
  
  def firstTraverse(x:Category,DF: DataFrame):Category={
    x.uri.map(x=>(x.URIslist=fetchObjectsOfSubject(DF,x.Uri)))
    x
  }
  def secondTraverse(xl: Category,DF: DataFrame):Category={
      for (fTR <- xl.uri){
        fTR.URIslist.map(x=>(x.URIslist=fetchObjectsOfSubject(DF,x.Uri)))
      }
    xl
  }
          /*for (sTR <- fTR.URIslist){
          //println(sTR.Uri)
          sTR.URIslist=fetchObjectsOfSubject(DF,sTR.Uri)
          //sTR.URIslist.foreach(x => println(x.Uri))
          /*for (tTR <- sTR.URIslist){
            println(sTR.Uri)
            tTR.URIslist=fetchObjectsOfSubject(DF,sTR.Uri)
            println(tTR.Uri)
          }*/
          //sTR.URIslist.map(x=>(x.URIslist=fetchObjectsOfSubject(DF,x.Uri)))
        }*/
  def appendToRDD(data: String) {
     val sc = spark.sparkContext
     val rdd = sc.textFile("Word2VecData")  
     val extraRDD=sc.parallelize(Seq(data))
     val newRdd = rdd ++ extraRDD
     //newRdd.map(_.toString).toDF.show()
     newRdd.map(_.toString).toDF.coalesce(1).write.format("text").mode("append").save("Word2VecData")
     //newRdd.map(_.toString).toDF.coalesce(1).write.format("text").mode("overwrite").save("Word2VecData")
  }
 
  def main(args: Array[String]) {
      val sc = spark.sparkContext
      //| Fetch Data
      val R=RDFApp.exportingData("src/main/resources/rdf.nt")
      
      //| Fetch Categories
      //> var myCategories = Categories.categories
      var fakeCategories = List("Hunebed", "Paddestoel", "Buswachten")
      
      //| Converting each category to a Category Object with the list of URIs belonging to it
      var categoryOBJs=fakeCategories.map(x => new Category(x,fetchAllOfWordAsSubject(R.toDF(),x)))
      // showCategoryObjects(categoryOBJs)

      //| Fetch the objects related to the URIs of each category
      var firstTR=categoryOBJs.map(x => firstTraverse(x,R.toDF()))
      // showFirstTraverse(firstTR)
      
      var secondTR=firstTR.map(x => secondTraverse(x,R.toDF()))
      showSecondTraverse(secondTR)
      


      // Stage one
      //val Res=fetchAllOfWordAsSubject(R.toDF(),"Hunebed")
      //Res.show(false)
                           
      /*
      val list = Res.select("Object").rdd.map(r => r(0)).collect()
      val stringlist = list.mkString(" ")
      list.foreach(line => println(line))
      println(stringlist)
      */
      /*
      val Org= sc.parallelize(Seq(stringlist))
      val headerRDD= sc.parallelize(Seq("<http://commons.dbpedia.org/resource/File:Hunebed_015.jpg> <http://commons.dbpedia.org/resource/File:Hunebed_013.jpg>"))
      val bodyRDD= sc.parallelize(Seq("BODY2"))
      val footerRDD = sc.parallelize(Seq("FOOTER"))
      val extraRDD=sc.parallelize(Seq("FOOTER"))
      val finalRDD = Org++ headerRDD ++ bodyRDD ++ footerRDD ++ extraRDD
			
      finalRDD.map(_.toString).toDF.coalesce(1).write.format("text").mode("overwrite").save("Word2VecData")
     //> appendToRDD("""<http://commons.dbpedia.org/resource/File:Paddestoel_002.jpg>""")
			*/
    println("~Stopping Session~")
    spark.stop()
  }
}

//Breadth First Search
      //finalRDD.foreach(line => println(line))
      
      //output to one file
      //finalRDD.coalesce(1, true).saveAsTextFile("testMie")
      //finalRDD.saveAsTextFile("out\\int\\tezt")


      /*
     val rdd = sc.textFile("Word2VecData")
     rdd.map(_.toString).toDF.show()
     val rddnew = rdd ++ headerRDD
     rddnew.map(_.toString).toDF.show()
   
     rddnew.map(_.toString).toDF.coalesce(1).write.format("text").mode("append").save("Word2VecData")
     */
     
     //val finalRDDD=rddnew.map(_.toString).toDF
     
    // val bodyRDxD= sc.parallelize(Seq("BODYx"))
     
     //bodyRDxD.map(_.toString).toDF.coalesce(1).write.format("text").mode("append").save("Word2VecData") // 'overwrite', 'append', 'ignore', 'error'.
      //finalRDD.map(_.toString).toDF.write.mode("append").text("testMie")