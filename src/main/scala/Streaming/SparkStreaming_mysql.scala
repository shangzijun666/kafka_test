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


object SparkStreaming_mysql {

  def main(args: Array[String]): Unit = {
    val prop_mysql = new Properties()
    val db_url ="jdbc:mysql://123.57.236.115:3306/test?zeroDateTimeBehavior=convertToNull&useSSL=false"
    val db_user="root"
    val db_passwrod = "123456"
    val db_driver = "com.jdbc.mysql.Driver"
    prop_mysql.setProperty("user", db_user)
    prop_mysql.setProperty("password", db_passwrod)
    prop_mysql.setProperty("driver", db_driver)
//    prop_mysql.setProperty("url", db_url)

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
            var conn: Connection = DriverManager.getConnection(db_url,prop_mysql)
            var stmt: PreparedStatement = null
            try {
            val sql = "insert into company(entid,entname,uniscid) values (?,?,?) on duplicate key update entname = ?,uniscid = ?"
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, entid)
            stmt.setString(2,entname)
            stmt.setString(3,uniscid)
            stmt.setString(4,entname)
            stmt.setString(5,uniscid)
            stmt.executeUpdate()
              } catch {
                case e: Exception => e.printStackTrace()
              } finally {
                if (stmt != null) {
                  stmt.close()
                }
                if (conn != null) {
                  conn.close()
                }
              }
            })
        }
      )


    ssc.start()
    ssc.awaitTermination()
  }

}
