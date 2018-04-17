package com.kafkastream.stream;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GenericAvroDeserializer implements Deserializer<GenericRecord>
{

    KafkaAvroDeserializer inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public GenericAvroDeserializer()
    {
        inner = new KafkaAvroDeserializer();
    }

    public GenericAvroDeserializer(SchemaRegistryClient client)
    {
        inner = new KafkaAvroDeserializer(client);
    }

    public GenericAvroDeserializer(SchemaRegistryClient client, Map<String, ?> props)
    {
        inner = new KafkaAvroDeserializer(client, props);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        inner.configure(configs, isKey);
    }

    @Override
    public GenericRecord deserialize(String s, byte[] bytes)
    {
        return (GenericRecord) inner.deserialize(s, bytes);
    }

    @Override
    public void close()
    {
        inner.close();
    }
}
