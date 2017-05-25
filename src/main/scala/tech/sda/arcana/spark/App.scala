package tech.sda.arcana.spark

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader

object App {

  def main(args: Array[String]) = {
    val input = "./src/main/resources/rdf.nt"

    println("======================================")
    println("|        Triple reader example       |")
    println("======================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example (" + input + ")")
      .getOrCreate()

    val triplesRDD = NTripleReader.load(sparkSession, new File(input))

    triplesRDD.take(5).foreach(println(_))

    sparkSession.stop

  }

}