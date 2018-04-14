package com.kafkastream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class TestEventsListner
{
    public static void main(String[] args)
    {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "customers_group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("order"));
        consumer.poll(0);
        //consumer.seekToBeginning(Arrays.asList(new TopicPartition("customer",0)));
        //consumer.seekToBeginning(consumer.assignment());
        consumer.seek(new TopicPartition("order",0),0);
        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(0);
            for (ConsumerRecord<String, String> record : records)
            {
                //System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println("Record: "+record.toString());
            }

        }
    }
}
