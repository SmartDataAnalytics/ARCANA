package tech.sda.arcana.spark.classification.cnn


class Core(model:String,train:String,label:String,test:String) extends BigDl_Spark {
  
  def initialize()={
    //initialize the SparkContext
    super.initialize(model)
    //Convert the training data to vectors
 
    //Create RDDs using the training data and the labels
    
    //Create RDDs for the test data
    
    
  }
  
  def run()={
    //Train the Data using the optimizer
    
  }
  
  def classify()={
    //Test the Data
    
  }
  
}