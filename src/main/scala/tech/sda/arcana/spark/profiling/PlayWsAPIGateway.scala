package tech.sda.arcana.spark.profiling

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import play.api.mvc._
import play.api.libs.ws._
import play.api.http.HttpEntity

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.ExecutionContext
import akka.actor.ActorSystem
import play.api.libs.ws.ahc.AhcWSClient

import scala.concurrent.Future
object PlayWsAPIGateway {
  import scala.concurrent.ExecutionContext.Implicits._
  def main(args: Array[String]) = {
 
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Arcana")
      .getOrCreate()
 

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val wsClient = AhcWSClient()

    call(wsClient,"http://words.bighugelabs.com/api/2/fe297721a04ca9641ae3a5b1ae3033a2/bottle/json")
      .andThen { case _ => wsClient.close() }
      .andThen { case _ => system.terminate() }

    sparkSession.stop

  }
   def call(wsClient: WSClient,url: String): Future[Unit] = {
    wsClient.url(url).get().map { response =>
      val statusText: String = response.statusText
      println(s"Got a response $statusText")

      val request: WSRequest = wsClient.url(url)
      val complexRequest: WSRequest =
        request.addHttpHeaders("Accept" -> "application/json")
          .addQueryStringParameters("search" -> "play")
         val futureResponse: Future[WSResponse] = complexRequest.get()    
     
         
      val result = Await.result(futureResponse, Duration.Inf)

      
      println(result.body)
    
       //println(s"Got a response $futureResponse")
    }
   }
}