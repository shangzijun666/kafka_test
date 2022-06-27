package com.stepone.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer_test {
    public static void main(String[] args) {
        //0 属性配置
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"123.57.236.115:9092");

        //指定对应的 key value 序列化类型

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //1 创建 生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //2.发送数据
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first","kafka_test"+i));
        }
        //3.关闭对象
        producer.close();
    }
}
