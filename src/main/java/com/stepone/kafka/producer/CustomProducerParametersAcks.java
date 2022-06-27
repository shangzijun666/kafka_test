package com.stepone.kafka.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class CustomProducerParametersAcks {
    public static void main(String[] args) {

        //配置
        Properties properties = new Properties();

        //集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"123.57.236.115:9092");
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG,10);
        //压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        //acks
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        //重试参数
        properties.put(ProducerConfig.RETRIES_CONFIG,3);

        //创建生产者
        KafkaProducer<String, String> kafkaproducer = new KafkaProducer<String,String>(properties);


        //发送数据
        for (int i = 0; i < 5; i++) {
            kafkaproducer.send(new ProducerRecord<>("first","基尼太美   ===> "+i));
        }


        //关闭
        kafkaproducer.close();
    }
}
