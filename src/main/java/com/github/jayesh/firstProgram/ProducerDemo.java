package com.github.jayesh.firstProgram;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
        String bootStrapServer = "127.0.0.1:9092";

        //Step 1 - create Producer property

        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // step 2 - Create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //step -3 create producer record
        int length = 10;
        boolean useLetters = true;
        boolean useNumbers = false;

        for(int i = 0 ; i<10;i++) {
            String value = RandomStringUtils.random(length, useLetters, useNumbers) + Integer.toString(i);
            String topic = "first_topic";
            String key = "ID_" + Integer.toString(i);

            logger.info("Key :" + key);


            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,  value );

            //step -4 send the data (async)
            //producer.send(record);

            //send data with call back -
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //run every time record is successfully sent

                    if (e == null) {
                        logger.info("received new meta data . \n" + "Topic :" + recordMetadata.topic() + "\n" +
                                "Partitions :" + recordMetadata.partition() + "\n" +
                                "Offset :" + recordMetadata.offset() + "\n" +
                                "Timestamp :" + recordMetadata.timestamp());

                    } else {
                        logger.error("Error log Producing", e);
                    }

                }
            }).get(); //block the send  to make synchronous -

        }
        //flush and close the producer
        producer.flush();
        producer.close();

    }
}
