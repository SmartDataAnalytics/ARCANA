package tech.sda.arcana.spark.profiling

import java.io.File
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader



object App {

  def main(args: Array[String]) = {

    println("==========================")
    println("|        Profiling       |")
    println("==========================")

    val category = new Category()    
    for ( x <- category.categories ) {
         println( x )
      }
    
  //  val pt =WSAPI(ws: WSClient);
  // val inst: WSAPI = new WSAPI()
  // inst.Operate()
    //val Junit = new JWI();
    //val x = new WSAPI();
    
		//Junit.printz();

  }

}
