package eu.cognet.dse;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *  Kafka Producer!
 *
 */

public class App 
{
    private static Scanner in;	
	
    public static void main( String[] argv )
    {
        System.out.println( "World Class!" );
        
        if (argv.length != 2) {
            System.err.println("Please specify 2 parameters ");
            System.err.println("input arg 1 topic are2 kafka host ");
            System.exit(-1);
        }
        String topicName = argv[0];
        String kafkaHost = argv[1]+":9092";
        System.out.println("Kafka Producer (type exit to quit)\n" + " host: " + kafkaHost + "  topic: " +  topicName);
        in = new Scanner(System.in);
 
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaHost);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
 
        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(configProperties);
        
        String line = in.nextLine();
        while(!line.equals("exit")) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, line);
            producer.send(rec);
            line = in.nextLine();
        }
        in.close();
        producer.close();
        
	}
}
