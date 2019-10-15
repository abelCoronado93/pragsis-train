import jdk.internal.net.http.frame.DataFrame;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.*;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class SparkStreamingEnrichment {

  static private String status_OK = "OK";
  static private String topic = "grupo01OK";
  static private JavaSparkContext sc = null;

  public static void main(String[] args) throws InterruptedException {
      SparkConf conf = new SparkConf();
      conf.setAppName("SparkStreamingConsumer");
      conf.setMaster("local[*]");

      sc = new JavaSparkContext(conf);

      // Spark Streaming config
      Map<String, Object> kafkaParams = new HashMap<String, Object>();
      kafkaParams.put("bootstrap.servers", "formacion02.pragsis.local:9092");
      kafkaParams.put("key.deserializer", StringDeserializer.class);
      kafkaParams.put("value.deserializer", StringDeserializer.class);
      kafkaParams.put("group.id", "consumer2019");
      kafkaParams.put("enable.auto.commit", true);

      Collection<String> topics = Arrays.asList(topic);

      JavaStreamingContext streamingContext = new JavaStreamingContext(sc, new Duration(10000));

      JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
        streamingContext,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams)
      );

      messages.foreachRDD(rdd -> {
        System.out.println("New RDD with " + rdd.partitions().size()  + " partitions and " + rdd.count() + " records");
        rdd.foreachPartition(record-> {
          record.forEachRemaining(message ->
            {
                if(message.key().equals(status_OK)) {
                    readHDFS(message.key(), message.value());
                }
            });
        });
      });

      streamingContext.start();
      streamingContext.awaitTermination();
  }

  private static void readHDFS(String key, String value){
    JSONObject obj = new JSONObject(value);
    String idUt = obj.getString("ut");
    Long timestamp = obj.getLong("ts");

    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(timestamp);

    int mYear = calendar.get(Calendar.YEAR);
    String mMonth = "" + calendar.get(Calendar.MONTH);
    if(mMonth.length()==1)
        mMonth="0"+mMonth;

    String mDay = ""+calendar.get(Calendar.DAY_OF_MONTH);
    if(mDay.length()==1)
        mDay = "0"+mDay;

    //sc.textFile("/user/luis.bartol/csv/arq/raw/ut="+idUt+"/year="+mYear+"/month="+mMonth+"/day="+mDay+"/*");
     //sc.textFile("/user/master/grupo1/dictionary_data/part-m-00000").map(line->line.split(",")).take(10);

    SQLContext sqlContext = new SQLContext(sc);

    sqlContext.sql("insert into table bd_prueba.tablastream ");

    //Dataset<Row> dfCSV = sqlContext.sql("select timestamp,ut from bd_prueba.ut_data limit 100");

    //JavaRDD<String> lines = sc.textFile("/user/master/grupo1/dictionary_data/part-m-00000");
    //String firstLine = lines.first();

    //System.out.println(firstLine);
  }
}
