package pack

import org.apache. spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql. functions._

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
    
//Spark code required for transforming read data
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    // initializing the streaming context ----3
    val ssc = new StreamingContext(conf, Seconds(2))
    
    //initialize the topic
    val topics = Array("sprktpk")
    
    //initialize kafka params   
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest"
    )
      
    //Kafka utils  - combining streamimg context, topics and kafka params
    val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
       )
       
    //fetching value and printing it, Kafka gives out a key value pair while the value section contains the stream data
    stream.map(record => (record.value)).print()
    
    //start the stream
    ssc.start()
    ssc.awaitTermination() //incase of any issues terminate it
    
  }

}