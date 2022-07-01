package Streaming
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel

import java.util.{Properties, Random}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkStreaming01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming01")

    val prop = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "82.156.164.171:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "sparkstreaming",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )


    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies Subscribe[String, String](Set("test1"), prop)
    )
    kafkaDS.map(_.value()).print()

    ssc.start()
    ssc.awaitTermination()

  }
}


