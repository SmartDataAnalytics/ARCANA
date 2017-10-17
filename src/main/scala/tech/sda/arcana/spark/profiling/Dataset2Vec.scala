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
  def printList(args: TraversableOnce[_]): Unit = {
      args.foreach(println)
  }
  def showCategories(){
      Categories.categories.foreach(line => println(line))//println(categories(1))
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
  def fetchAllOfWordAsObject(DF: DataFrame, word: String):List[URI]={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT * from triples where Object like '%$word%'") 
      val UriList=Res.select("Object").rdd.map(r => r(0)).collect()
      UriList.toList.distinct.map(x => new URI(x.asInstanceOf[String]))
  }
  
  def fetchAllOfWordAsSubject(DF: DataFrame, word: String):List[URI]={
      DF.createOrReplaceTempView("triples")
      val Res = spark.sql(s"SELECT * from triples where Subject like '%$word%'") 
      val UriList=Res.select("Subject").rdd.map(r => r(0)).collect()
      //printList(UriList)
      UriList.toList.distinct.map(x => new URI(x.asInstanceOf[String]))
  }
  
  def firstTraverse(x:Category,DF: DataFrame){
    x.uri.map(x=>(x.URIslist=fetchAllOfWordAsObject(DF,x.Uri)))
  }
  
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

      var newCategories=fakeCategories.map(x => new Category(x,fetchAllOfWordAsSubject(R.toDF(),x)))
      /*
      for (name <- newCategories){
        println(name.Category)
        name.uri.foreach(line => println(line.Uri))
      }*/
      var newnewCategories=newCategories.map(x => firstTraverse(x,R.toDF()))


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