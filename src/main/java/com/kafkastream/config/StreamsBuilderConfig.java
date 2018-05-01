package com.kafkastream.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class StreamsBuilderConfig
{
    private static StreamsBuilder streamsBuilder=new StreamsBuilder();

    private static Topology topology = streamsBuilder.build();

    private static KafkaStreams streams=new KafkaStreams(topology, getProperties());

    private StreamsBuilderConfig()
    {

    }

    public static StreamsBuilder getInstance()
    {
        return  streamsBuilder;
    }

    public static KafkaStreams  getStreams()
    {
        return streams;
    }

    private static Properties   getProperties()
    {
        Properties properties = new Properties();
        properties.put("application.id", "cqrs-streams");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("commit.interval.ms", "2000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.deserializer", Serdes.String().deserializer().getClass());
        properties.put("value.deserializer", SpecificAvroDeserializer.class);

        return properties;
    }

}
