package tech.sda.arcana.spark.representation

object sentencesToTensors {
  //here bunch of questions (text file) is going to be converted to a bunch (table) 
  //of Tensors (each layer of the tensor will be regarding a representation)
  val t=Seq(("Mohamad",(1,57)),("Mohama",(2,58)),("Moham",(3,59)),("Moham",(4,60)))
  t.foreach(println(_))
}