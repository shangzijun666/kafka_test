package Streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import com.alibaba.fastjson.JSON

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.sql.SparkSession

import java.util.Properties
import javax.xml.bind.DatatypeConverter.parseTime


object SparkStreaming_es {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val context = new SparkContext(conf)

    val ssc = new StreamingContext(context, Seconds(3))

    val prop = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"123.57.236.115:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->"org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->"org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG->"streaming01",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG->"earliest"
    )

    val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("spark_test"), prop)
    )
    val resultRDD = value.map(_.value())

//    把DStream保存到MySQL数据库中
    resultRDD.foreachRDD(
        rdd =>if (!rdd.isEmpty()){
          rdd.foreach(line=>{
            var entid = JSON.parseObject(line).getString("entid")
            var entname = JSON.parseObject(line).getString("entname")
            var uniscid = JSON.parseObject(line).getString("uniscid")

            val company_struct = StructType(
                Array(
                  StructField("ent_id", StringType, nullable = true),
                  StructField("entname", StringType, nullable = true),
                  StructField("uniscid", StringType, nullable = true)
                )
              )

            })
        }
      )


    ssc.start()
    ssc.awaitTermination()
  }

}
