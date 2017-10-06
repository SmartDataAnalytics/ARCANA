package tech.sda.arcana.spark.representation

// $example on$
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Word2VecUpdatedExample {
    def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Word2VecUpdatedExample")
      .getOrCreate()

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "B Bridget remembered the nightmthat her parents died in the fire her younger sister Brandy and her was at a sleepover at a church youth camp. She remembered that their youth pastor took her aside while one of the women sat with Brandy and was informed of their deaths. It was her call to tell Brandy about their parents. One of their Sunday school teachers took them to their home. It was burned down to the ground with only the fireplace till standing but in bad shape.".split(" "),
      "The fire had spread to the family's un attached tool shed but left the garden house and un attached garage. Bridget told the authorities about a will that was in a bank safety deposit box and another one with their lawyer named Mack something. The authorities knew who Mack was and got a copy of their will. In the will they had no relatives so and they requested that a friend of theirs and a church member would finish raising the girls. The family that they had choose left the country on a three year mission trip. The judge ordered the girls into faster care.".split(" "),
      "With Brandy being eight years old and in elementry school adjusted to the school and had many friends that would continue their schooling in the same grade Brandy would. It was a lot harder for Bridget. With her looks she had boys interested in her but was shunned by the girls. She wa surrounded by some of the girls in the shower room and they cut her hair. Then they posted bad things about her using poster paper. They took her picture and made posters that she was available for a good time and used the foster parents telephone number ".split(" "),
      "I wish Java Matrix could use case classes".split(" "),
      "Java Matrix is a new technology that is awesome".split(" "),
      "I am going to buy the Java Matrix".split(" "),
      "I love Java".split(" "),
      "I love Matrix".split(" "),
      "Java is sun".split(" "),
      "Matrix is sun".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
  
    val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
     
    val result = model.transform(documentDF)
     println("Vectors!") 
    //result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
    //  println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }
    // $example off$
    println("End") 
    
    val synonyms = model.findSynonyms("school",1000)
    synonyms.show()
    //println(synonyms.word)
 

    spark.stop()
  }
}
// scalastyle:on println