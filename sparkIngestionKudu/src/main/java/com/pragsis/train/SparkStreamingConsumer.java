package com.pragsis.train;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 *  Consumidor SparkStreaming
 */
public class SparkStreamingConsumer {

    private final static String BOOTSTRAP_SERVERS = "formacion02.pragsis.local:9092";
    private final static String GROUP_ID = "consumer1";
    private final static String TOPIC_CONSUM = "grupo01";
    private final static String TOPIC_PRODUCE_OK = "grupo01OK";
    private final static String TOPIC_PRODUCE_KO = "grupo01KO";
    private final static long DURATION = 10000;

    private static final String[] fieldsJson = {"ts","ut","varName","value"};


    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingConsumer");
        conf.setMaster("local[2]");

        //configuración spark streaming
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", GROUP_ID);
        //kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList(TOPIC_CONSUM);

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(DURATION));

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        //configuración productor que envía los mensajes recibidos del streaming a topic grupo01OK y grupo01KO
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        messages.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()  + " partitions and " + rdd.count() + " records");
            rdd.foreachPartition(record-> {
                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
                record.forEachRemaining(message -> {
                        if(validarJson(message)) {
                            producer.send(new ProducerRecord<String, String>(TOPIC_PRODUCE_OK, message.key(), message.value()));
                            System.out.println("--> OK: Producer: " + message.key() + " message: " + message.value()+" <--");
                        }
/*                        else{
                            producer.send(new ProducerRecord<String, String>(TOPIC_PRODUCE_KO, message.key(), message.value()));
                            System.out.println("KO: Producer: " + message.key() + " message: " + message.value());
                        }*/
                    });
                producer.close();
            });
        });
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    /**
     * Validador JSON entrada
     *
     * @param record
     * @return
     */
    private static Boolean validarJson(ConsumerRecord<String, String> record){
        JSONObject jsonObject = new JSONObject(record.value());
        int i = 0;
        while (i < fieldsJson.length){
            try {
                if (StringUtils.isNotBlank(jsonObject.get(fieldsJson[i].toString()).toString())){
                    i++;
                }
            }catch(JSONException ex){
                return false;
            }
        }
        return true;
    }
}
