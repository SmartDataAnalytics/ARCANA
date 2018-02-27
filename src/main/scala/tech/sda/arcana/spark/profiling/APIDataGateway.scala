package tech.sda.arcana.spark.profiling

//import io.circe._, io.circe.parser._
import net.liftweb.json.Xml.{toJson, toXml}
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
 * An Object that is responsible for the communication with other APIs
 */

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

  def getSynomyns_bighugelabs(expression:String){
    implicit val formats = net.liftweb.json.DefaultFormats
    val result = fetch(s"http://words.bighugelabs.com/api/2/fe297721a04ca9641ae3a5b1ae3033a2/$expression/json")
    val json = parse(result)
    val Z = json.extract[Obj] 
    Z.noun.syn.foreach { println }
    println("------------------------------------")
    Z.verb.syn.foreach { println }
  }
  def getSynomyns_merriam(expression:String){
    val result2 = fetch(s"http://www.dictionaryapi.com/api/v1/references/thesaurus/xml/$expression?key=e8c94890-746e-4df8-98da-08cdf5d84e53")
    print(result2)
    println("===========================================")
    val tx = scala.xml.XML.loadString(result2)
    val json = toJson(tx)
    //pretty(render(json))

    val strings = for {
        e <- tx.child
      
    } println( (e \\ "syn").text)
  }
  
      @throws(classOf[java.io.IOException])
    @throws(classOf[java.net.SocketTimeoutException])
    def get(url: String,
            connectTimeout: Int = 5000,
            readTimeout: Int = 5000,
            requestMethod: String = "GET") =
    {
        import java.net.{URL, HttpURLConnection}
        val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
        connection.setConnectTimeout(connectTimeout)
        connection.setReadTimeout(readTimeout)
        connection.setRequestMethod(requestMethod)
        val inputStream = connection.getInputStream
        val content = scala.io.Source.fromInputStream(inputStream).mkString
        if (inputStream != null) inputStream.close
        content
    }
  def getRankUclassify(expression:String):(String,String)={
    implicit val formats = net.liftweb.json.DefaultFormats
    val Nexpression=expression.trim().replaceAll(" ", "+")
    // L5ZjO3PO2YlO || ZS0M9b0sQN1k || 8hk7Sw5l8jKf
    val query = s"https://api.uclassify.com/v1/uClassify/Sentiment/classify/?readKey=8hk7Sw5l8jKf&text=$Nexpression"
    
    //def get(url: String) = scala.io.Source.fromURL(query).mkString
    val result3 = get(query)
    var Negative =""
    var Positive =""
    val pattern = """negative\":(\d+\.\d+),\"positive\":(\d+\.\d+)""".r
    pattern.findAllIn(result3).matchData foreach {
       m => Negative=m.group(1)
       Positive=(m.group(2))
    }
    (Negative,Positive)
  }
  import scala.sys.process._
  def fetchDbpediaSpotlight(term:String):String={
      val cmd = Seq("curl",s"http://api.dbpedia-spotlight.org/en/annotate?text=$term.&confidence=0&support=0","-H", "Accept:application/json")
      val res = cmd.!!
      val UriReg = raw"""(?<=@URI\"\:\")(.*?)(\"\,\")""".r
      val resulturi=UriReg.findFirstIn(res)
      //println(resulturi.mkString.dropRight(3))
      return resulturi.mkString.dropRight(3)
  }
  def main(args: Array[String]) = {

        val word = "good"
        val result = getRankUclassify(word)
        
        println(s"word: $word, "+"Negative: "+result._1+", Positive: "+result._2)
        println((result._2.toDouble - result._1.toDouble))
        //println(fetchDbpediaSpotlight("Kill"))
        /*
        getRankUclassify("kill")
        getRankUclassify("hunt")
        getRankUclassify("terrorist")
        getRankUclassify("blast")
        getRankUclassify("spy")
        getRankUclassify("run")
        getRankUclassify("move")
        getRankUclassify("good")
        getRankUclassify("test")
        getRankUclassify("run")
        getRankUclassify("walk")
        getRankUclassify("jump")
        getRankUclassify("rap")
        getRankUclassify("brag")
        getRankUclassify("dump")
        getRankUclassify("slump")*/
      //getSynomyns_bighugelabs("bomb")
      //getSynomyns_merriam("war")

  }
}
    //val jsonAst = result.parseJson // or JsonParser(source)
    //val json = jsonAst.prettyPrint // or .compactPrint
    //println(json)

    //val parsed = JSON.parseFull(result)
    //val jsonAst = result.parseJson // or JsonParser(source)
    //val json = jsonAst.prettyPrint // or .compactPrint