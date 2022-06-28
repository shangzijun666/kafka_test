package com.stepone.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CustomConsumerSeekTime {
    public static void main(String[] args) {
        //0.配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"123.57.236.115:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test2");
        //1.创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(properties);

        //2.订阅主题
        ArrayList<String> topic = new ArrayList<String>();
        topic.add("first");
        consumer.subscribe(topic);


        //指定位置进行消费
        Set<TopicPartition> assignment = consumer.assignment();
        //保证分区方案制定完毕
        while (assignment.size()==0){
            consumer.poll(Duration.ofSeconds(1));
            assignment = consumer.assignment();
        }
        //把时间转换成对应的offset
        HashMap<TopicPartition, Long> map = new HashMap<>();
        //封装集合
        for (TopicPartition topicPartition : assignment) {
            map.put(topicPartition,System.currentTimeMillis()-1*24*3600*1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(map);

        for (TopicPartition topicPartition : assignment) {
            //指定offset
//            consumer.seek(topicPartition,100);
            OffsetAndTimestamp offsetTimestamp = offsets.get(topicPartition);
            consumer.seek(topicPartition,offsetTimestamp.offset());
        }

        //3.消费数据
        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record);
            }
        }

    }
}
