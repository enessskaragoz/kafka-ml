import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;

public class KafkaConsumer {

    public static void main(String[] args) throws InterruptedException {

        // Kafka topic and schema registry URL
        String topic = "tweets";
        String schemaRegistryURL = "http://localhost:8081";

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("KafkaConsumer").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Kafka parameters
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("schema.registry.url", schemaRegistryURL);
        kafkaParams.put("specific.avro.reader", "true");

        // Create a Kafka stream
        HashSet<String> topicsSet = new HashSet<>();
        topicsSet.add(topic);
        JavaDStream<Object> kafkaStream = KafkaUtils.createDirectStream(
                ssc,
                Object.class,
                Object.class,
                io.confluent.kafka.serializers.KafkaAvroDecoder.class,
                io.confluent.kafka.serializers.KafkaAvroDecoder.class,
                kafkaParams,
                topicsSet
        ).map(message -> message.message());

        // Convert the stream to a DataFrame and write it to Hive
        kafkaStream.foreachRDD(rdd -> {
            Dataset<Row> df = spark.read().format("avro").load(schemaRegistryURL + "/subjects/" + topic + "-value/versions/latest");
            df.write().mode(SaveMode.Append).saveAsTable("tweets");
        });

        // Start the streaming context and wait for termination
        ssc.start();
        ssc.awaitTermination();
    }
}
