package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//can refer to this website for base template
//https://spark.apache.org/docs/2.2.0/streaming-kafka-0-10-integration.html
//imports for spark streaming  + kafka integration  --- 1
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

//contains StreamingContext class and Seconds() --- 2
import org.apache.spark.streaming._

object obj {

  def main(args: Array[String]): Unit = {
    
    //required for writing the file from eclipse
    System.setProperty("hadoop.home.dir","D:\\hadoop")
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate( )
    import spark.implicits._
    
    //kafka integration with spark starts -----------------------
    
    // initializing the streaming context and set for how many seconds spark hits kafka ---- 3
    val ssc = new StreamingContext(conf, Seconds(2))
    
    //initialize the topic name ---- 4
    val topics = Array("consumption_method_check")
    
    //initialize kafka params ---- 5
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer3",
      "auto.offset.reset" -> "earliest"
    )
     
    //RDD Streaming
    //Kafka utils  - combining streamimg context, topics and kafka params. RDD/D Streaming ---- 6
    val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
       )
    
    //fetching value and printing it, Kafka gives out a key value pair while the value section contains the stream data
    val streamdata = stream.map(x => (x.value)) //RDD operation
    
    //To process streamed data, you have to use foreachRDD()
    streamdata.foreachRDD(x=>
      if(!x.isEmpty()){
        val df = x.toDF("value") //converting RDD to dataframe
        val df_withtime = df.withColumn("timestamp",current_timestamp) //Creating a new column and attaching time stamp
        df_withtime.show(false) //prints out the CLI producer input/Nifi input with time stamp
      }
    
    )
    
    
    //start the stream
    ssc.start()
    ssc.awaitTermination() //incase of any issues terminate it
    
  }

}