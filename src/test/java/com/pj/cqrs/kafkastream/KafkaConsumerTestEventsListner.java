package com.pj.cqrs.kafkastream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerTestEventsListner
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
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("order"));
        consumer.poll(Duration.ZERO);
        //consumer.seekToBeginning(Arrays.asList(new TopicPartition("customer",0)));
        //consumer.seekToBeginning(consumer.assignment());
        consumer.seek(new TopicPartition("order",0),0);
        while (true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ZERO);
            for (ConsumerRecord<String, String> record : records)
            {
                System.out.printf("customer offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println();
            }

        }
    }
}
