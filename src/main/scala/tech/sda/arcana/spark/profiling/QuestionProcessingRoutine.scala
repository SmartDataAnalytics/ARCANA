package tech.sda.arcana.spark.profiling
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
object QuestionProcessingRoutine {
  val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Dataset2Vec")
      .getOrCreate()
      
  // Simple Question Tokenizing
  def tokenizeQuestion(question: String){
    /*
     * in case of many question you can do:
      val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
  	  )).toDF("id", "sentence")
     */
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, question)
    )).toDF("id", "sentence")
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val countTokens = udf { (words: Seq[String]) => words.length }
    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("sentence", "words")
        .withColumn("tokens", countTokens(col("words"))).show(false)
    removeStopWords(tokenized)
  }
  // Question Tokenizing while using Regex
  def tokenizeQuestionWithRegex(question: String){
    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, question)
    )).toDF("id", "sentence")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)
    val countTokens = udf { (words: Seq[String]) => words.length }
    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    regexTokenized.select("sentence", "words")
        .withColumn("tokens", countTokens(col("words"))).show(false)
  }
  
  def removeStopWords (DF: DataFrame){
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")
    
    remover.transform(DF).show(false)
  }
  def main(args: Array[String]) = {
   
    tokenizeQuestion("Hi There How are you? There was a car walking by a dog nearby the horse")    
    tokenizeQuestionWithRegex("Hi There How are you?")  

   } 
}