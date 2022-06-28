package com.stepone.kafka.common;

import com.stepone.kafka.producer.Producer_test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {


//        InputStream ins = ClassLoader.getSystemResourceAsStream("util/db.properties");
//        Properties property = new Properties();
//        property.load(ins);
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        System.out.println("1111111111111---->:");

        InputStream in = Producer_test.class.getClassLoader().getResourceAsStream("application.properties");

        properties.load(in);

        System.out.println("1111111111111---->:"+properties.getProperty("kafka.cluster"));
    }



}
