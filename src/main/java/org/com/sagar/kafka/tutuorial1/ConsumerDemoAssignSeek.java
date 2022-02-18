package org.com.sagar.kafka.tutuorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        // create consumer properties
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first-topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // earliest/latest/none

        // create a consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic

        //consumer.subscribe(Collections.singleton("first-topic"));  // single topic
        //consumer.subscribe(Arrays.asList("first-topic","second-topic")); // multiple topics
        //consumer.subscribe(Arrays.asList(topic));

        // assign and seek used to replay data or fetch a specific message

        // Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToRead = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToRead);

        int numberOfMessage = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;
        // poll for new data
        while (keepOnReading){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records){
                numberOfMessageReadSoFar += 1;
                logger.info("key: "+ record.key() + " , value: "+record.value());
                logger.info("Partition: "+ record.partition());
                logger.info("Offset: "+record.offset());
                if(numberOfMessageReadSoFar > numberOfMessage){
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("EXITING");


    }
}
