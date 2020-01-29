package com.nisum;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.RDDJavaFunctions;
import javafx.util.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Hello world!
 *
 */
public class App implements Serializable
{
    public static void main( String[] args )
    {
        //Spark Streaming
        SparkConf config=new SparkConf();
                config.setAppName("WordCounting");
                config.set("spark.cassandra.connection.host","127.0.0.1");

        try(JavaStreamingContext streamingContext=new JavaStreamingContext(config, Durations.seconds(30))) {
            //Kafka configs

            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            kafkaParams.put("metadata.broker.list", "localhost:9092");
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);

            Collection<String> topic = Arrays.asList("messages");
            System.out.println("topics----------"+topic);
            JavaInputDStream<ConsumerRecord<String, String>> messages =
                    KafkaUtils.createDirectStream(
                            streamingContext,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(topic, kafkaParams)
                    );
            System.out.println("----------------------------------------------------------------------------------------------");
             System.out.println("----------------------messages--------------------:"+messages);
            JavaPairDStream<String, String> results = messages
                    .mapToPair(record -> new Tuple2<>(record.key(), record.value()));
            System.out.println("----------------results-----------------:"+results);

           // JavaDStream<String> lines = results\
                //    .map(tuple2 -> tuple2._2());

            JavaDStream<String> lines = messages.map(ConsumerRecord::value);
            System.out.println("----------------lines---------------------:"+lines);
            JavaDStream<String> words = lines
                    .flatMap(x -> Arrays.asList(x.split("\\S+")).iterator());
            System.out.println("-------------words------------------:"+words);

            JavaPairDStream<String, Integer> wordCount = words.mapToPair(
                    s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
            System.out.println("--------wordCount-------------:"+wordCount);



            //writing into cassandra
            wordCount.foreachRDD(
                    javaRdd -> {
                        System.out.println("----------Inside javaRdd--------");
                        Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
                        System.out.println("----------wordCountMap--------"+wordCountMap);
                        for (String key : wordCountMap.keySet()) {
                            List<Word> wordList = Arrays.asList(new Word(key+"hi", wordCountMap.get(key)));
                            System.out.println("wordList------:"+wordList);
                            JavaRDD<Word> rdd = streamingContext.sparkContext().parallelize(wordList);
                            System.out.println("rdd------:"+rdd);
                            CassandraJavaUtil.javaFunctions(rdd)
                                    .writerBuilder("vocabulary", "words", CassandraJavaUtil.mapToRow(Word.class))
                                    .saveToCassandra();
                        }
                    }
            );
            streamingContext.start();
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
