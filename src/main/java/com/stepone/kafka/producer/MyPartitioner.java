package com.stepone.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //转换数据  stepone hello

        String msg = value.toString();
        int partition;
        if (msg.contains("stepone")||msg.contains("hello")){
            partition = 0;
        }else{
            partition = 1;
        }
        return partition;


    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
