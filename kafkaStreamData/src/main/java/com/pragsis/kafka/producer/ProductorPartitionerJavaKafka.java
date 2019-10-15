package com.pragsis.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProductorPartitionerJavaKafka {

    private static final String BROKER_SERVER = "formacion02.pragsis.local:9092";

    public static void main(String[] args) {

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", BROKER_SERVER);
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("partitioner.class", "com.pragsis.kafka.partitioner.CustomPartitioner");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        String nameTopic = "prueba-part-daniel.jimenez";
        int partition;
        for (int i = 0; i < 10000; i++) {
//            if  (i % 2 == 0 ){ // PAR
//                partition = 0;
//            }else{  // IMPAR
//                partition = 1;
//            }
//            ProducerRecord<String, String> record = new ProducerRecord<String, String>(nameTopic, partition, String.valueOf(i), String.valueOf(i));
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(nameTopic, String.valueOf(i),  String.valueOf(i));
            producer.send(record);
        }
        producer.close();
    }
}
