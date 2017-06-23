/*package tech.sda.arcana.spark.profiling

import javax.inject.Inject
import scala.concurrent.Future
import scala.concurrent.duration._

import play.api.mvc._
import play.api.libs.ws._
import play.api.http.HttpEntity

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.ExecutionContext

class WSAPI @Inject() (ws: WSClient) extends Controller {

   def main(args: Array[String]) {
    println("Hello, World!")
  }
  
  def Operate(){
    println("HI")
    /*
      val request: WSRequest = ws.url("http://words.bighugelabs.com/api/2/fe297721a04ca9641ae3a5b1ae3033a2/bottle/json")
      val complexRequest: WSRequest =
      request.withHeaders("Accept" -> "application/json")
        .withRequestTimeout(10000.millis)
        .withQueryString("search" -> "play")
    
    //val futureResponse: Future[WSResponse] = complexRequest.get()
    val url="http://words.bighugelabs.com/api/2/fe297721a04ca9641ae3a5b1ae3033a2/bottle/json"
    val futureResult: Future[String] = ws.url(url).get().map {
      response =>
        (response.json \ "person" \ "name").as[String]
}*/
  }

    
   
  
}*/