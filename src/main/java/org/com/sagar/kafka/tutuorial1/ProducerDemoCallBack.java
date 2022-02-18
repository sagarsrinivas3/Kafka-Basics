package org.com.sagar.kafka.tutuorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBack {
    public static void main(String[] args) {
        System.out.print("Program Started");
        Logger logger =LoggerFactory.getLogger(ProducerDemoCallBack.class);
        Logger log = LoggerFactory.getLogger(ProducerDemoCallBack.class);

        String bootstrapServer = "127.0.0.1:9092";

        // create producer properties
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i =0; i<10; i++) {
            // send data
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first-topic", "hello world sagar"+i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute everytime a record is successfully sent or an exception is thrown
                    if (e == null) {
                        //the record is successfully sent
                        log.info("\nreceived meta data" +
                                "\nTopic: " + recordMetadata.topic() +
                                "\nPartition: " + recordMetadata.partition() +
                                "\nOffset: " + recordMetadata.offset() +
                                "\nTimestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("\nError while producing", e);
                        e.printStackTrace();
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }
}
