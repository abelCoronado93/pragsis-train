package com.pragsis.kafka.producer;

import com.sun.tools.javac.util.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProductorJavaKafka {

    private static final String BROKER_SERVER = "formacion02.pragsis.local:9092";

    public static void main(String[] args) {

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", BROKER_SERVER);
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        String nameTopic = "test-daniel.jimenez";
        for (int i = 0; i < 10000; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(nameTopic, String.valueOf(i));
            producer.send(record);
        }
        producer.close();
    }

}
