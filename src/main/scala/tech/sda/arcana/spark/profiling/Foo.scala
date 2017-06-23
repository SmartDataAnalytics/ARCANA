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

class Foo @Inject() (ws: WSClient) extends Controller {
  def operate(){
  val url = "http://words.bighugelabs.com/api/2/fe297721a04ca9641ae3a5b1ae3033a2/bottle/json"
  val request: WSRequest = ws.url(url)
  val complexRequest: WSRequest =
  request.withHeaders("Accept" -> "application/json")
    .withRequestTimeout(10000.millis)
    .withQueryString("search" -> "play")
    val futureResponse: Future[WSResponse] = complexRequest.get()
  println("DONE")
  }
}*/