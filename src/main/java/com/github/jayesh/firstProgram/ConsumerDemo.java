package com.github.jayesh.firstProgram;

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

public class ConsumerDemo {

    public static void main(String[] args) {
        System.out.println("hello bhavi");
        String bootstrapServer = "127.0.0.1:9092";
        String group_id = "my-second-application";

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer );
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");



        //create consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        //subscribe consumer to our topic

        consumer.subscribe(Arrays.asList("first_topic"));


        //poll the new data
        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); //new in kafka 2.0
            for(ConsumerRecord record : records){
                logger.info("Key :" + record.key() + "value is " + record.value());
                logger.info("Partitions :" + record.partition() + "\n" + "Offset is :" + record.offset());

            }
        }


    }
}
