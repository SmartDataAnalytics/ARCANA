/*import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.ws._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.ws._
import play.api.libs.ws.ahc.AhcWSClient
import scala.concurrent.Future

object FooApp {
   
  lazy val ws = {
  import com.typesafe.config.ConfigFactory
  import play.api._
  import play.api.libs.ws._
  import play.api.libs.ws.ahc._
  import play.api.libs.ws.ahc.AhcConfigBuilder
  import org.asynchttpclient.AsyncHttpClientConfig

  val configuration = Configuration.reference ++ Configuration(ConfigFactory.parseString(
    """
      |ws.followRedirects = true
    """.stripMargin))

  val parser = new WSConfigParser(configuration, environment)
  val config = new AhcWSClientConfig(wsClientConfig = parser.parse())
  val builder = new AhcConfigBuilder(config)
  val logging = new AsyncHttpClientConfig.AdditionalChannelInitializer() {
    override def initChannel(channel: io.netty.channel.Channel): Unit = {
      channel.pipeline.addFirst("log", new io.netty.handler.logging.LoggingHandler("debug"))
    }
  }
  val ahcBuilder = builder.configure()
  ahcBuilder.setHttpAdditionalChannelInitializer(logging)
  val ahcConfig = ahcBuilder.build()
  new AhcWSClient(ahcConfig)
}
}*/