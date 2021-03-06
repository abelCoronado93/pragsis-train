package com.pragsis.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public class ConsumerJavaManualCommitKafka {

    private static final String BROKER_SERVER = "formacion02.pragsis.local:9092";

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", BROKER_SERVER);
        prop.put("group.id", "consumer-group1");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("enable.auto.commit", "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        String topicName = "prueba-part-daniel.jimenez";
        consumer.subscribe(Collections.singletonList(topicName));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1);
            for (ConsumerRecord<String, String> consumerRecord : records) {
                consumer.seek(consumer.assignment().iterator().next(),1000);
                System.out.println("Topic " + consumerRecord.topic() +
                        " Partition " + consumerRecord.partition() +
                        " Offset " + consumerRecord.offset() +
                        " Key " + consumerRecord.key() +
                        " Value " + consumerRecord.value());
            }
            consumer.commitSync();
        }
    }

}
