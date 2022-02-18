package org.com.sagar.kafka.tutuorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());

        // create consumer properties
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fifth-app";
        String topic = "first-topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // earliest/latest/none

        // create a consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic

        //consumer.subscribe(Collections.singleton("first-topic"));  // single topic
        //consumer.subscribe(Arrays.asList("first-topic","second-topic")); // multiple topics
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records){
                logger.info("key: "+ record.key() + " , value: "+record.value());
                logger.info("Partition: "+ record.partition());
                logger.info("Offset: "+record.offset());
            }
        }
        // Run multiple times - 1.click on class  2.edit run configurations 3.Modify options 4.Allow multiple instances


    }
}
