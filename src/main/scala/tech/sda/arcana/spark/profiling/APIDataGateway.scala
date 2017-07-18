package tech.sda.arcana.spark.profiling

//import io.circe._, io.circe.parser._

import net.liftweb.json._
import java.io._
import scala.util.parsing.json._
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SparkSession;
import java.net.{URI => JavaURI}
import scala.collection.mutable
//import spray.json._
//import DefaultJsonProtocol._ // if you don't supply your own Protocol (see below)

/*
 * My Keys -> merriam webster      
      Key (Dictionary):7a94d88d-4647-416b-bb3c-74bed96d4188
      Key (Thesaurus): e8c94890-746e-4df8-98da-08cdf5d84e53
 * 
 * My Keys -> Big Huge Thesaurus
 *    Key is fe297721a04ca9641ae3a5b1ae3033a2 
 *    
 * My Keys -> uclassify.com
 *    Key is L5ZjO3PO2YlO
 */

/*
 * An Object to fetch information from different APIs, for example to get the sentiment analysis
 * This can be improved to include delays and timeout fixing technique
 * I have a branch that implement a more advanced way than this and which needs to be completed if needed
 */

object APIData {
  
  // 1st way to do it
  @throws(classOf[java.io.IOException])
  def fetch(url: String) = scala.io.Source.fromURL(url).mkString
  
  
  case class noun(syn: List[String],ant: List[String])
  case class verb(syn: List[String],ant: List[String])
  case class Obj(noun:noun,verb:verb)

  def getSynomyns_bighugelabs(expression:String){
    implicit val formats = net.liftweb.json.DefaultFormats
    val result = fetch(s"http://words.bighugelabs.com/api/2/fe297721a04ca9641ae3a5b1ae3033a2/$expression/json")
    //val parsed = JSON.parseFull(result)
    //val jsonAst = result.parseJson // or JsonParser(source)
    //val json = jsonAst.prettyPrint // or .compactPrint
    val json = parse(result)
    val Z = json.extract[Obj] 
     Z.verb.syn.foreach { println }
    //println(Z.noun.syn)
    //val nOBJ = N.extract[Obj]
    
    //val parseResult = parse(result)
    //println(parseResult)
  }
  
  
  def main(args: Array[String]) = {
  
    println("============================")
    println("|        API Gateway       |")
    println("============================")
    

    getSynomyns_bighugelabs("war")
    //println(parsed)
    /*
    val jsonAst = result.parseJson // or JsonParser(source)
    val json = jsonAst.prettyPrint // or .compactPrint
    //println(json)
    
    val result2 = fetch("http://www.dictionaryapi.com/api/v1/references/thesaurus/xml/war?key=e8c94890-746e-4df8-98da-08cdf5d84e53")
    //println(result2)
    val x= "How+to+kill+a+person?"
    val result3 = fetch("https://api.uclassify.com/v1/uClassify/Sentiment/classify/?readKey=L5ZjO3PO2YlO&text="+x)
    println(result3)
    */
  }
}