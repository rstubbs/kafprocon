package eu.cognet.dse;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

/**
 *  Kafka consumer!
 *
 */

public class App {
    public static void main( String[] argv ) {
 	System.out.println( "World Class!" );

        if (argv.length != 2) {
            System.out.println("Please specify 2 parameters ");
            System.out.println("input arg 1 =  topic are2 kafka =  host ");
            System.exit(-1);
        }
 	String topicName = argv[0];
        String kafkaHost = argv[1]+":2181";

    Properties config = new Properties();
    config.put("zookeeper.connect", kafkaHost);
    config.put("group.id", "default");
    config.put("partition.assignment.strategy", "roundrobin");
    config.put("bootstrap.servers", kafkaHost);
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    kafka.consumer.ConsumerConfig consumerConfig = new kafka.consumer.ConsumerConfig(config);
    System.out.println("Kafka consumer (type exit to quit)\n" + " host: " + kafkaHost + "  topic: " +  topicName);


    ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
 
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topicName, 1);
 
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
 
    List<KafkaStream<byte[], byte[]>> streamList = consumerMap.get(topicName);
 
    KafkaStream<byte[], byte[]> stream = streamList.get(0);
 
    ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
    while(iterator.hasNext()) {
      System.out.println(new String(iterator.next().message()));
    }
  }
 
// public static void processRecords(Map<String, ConsumerRecords<String, String>> records) {
 //   List<ConsumerRecord<String, String>> messages = records.get("testTopic").records();
  //  if(messages != null) {
   //   for (ConsumerRecord<String, String> next : messages) {
    //    try {
     //     System.out.println(next.value());
      //  } catch (Exception e) {
       //   e.printStackTrace();
        //}
      //}
//    } else {
 //     System.out.println("No messages");
  //  }
   // }
}
