package com.kafkastream.stream;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;

public class SpecificAvroDeserializer<T extends org.apache.avro.specific.SpecificRecord> implements Deserializer<T>
{

    KafkaAvroDeserializer inner;

    /**
     * Constructor used by Kafka Streams.
     */
    public SpecificAvroDeserializer() {
        inner = new KafkaAvroDeserializer();
    }

    public SpecificAvroDeserializer(SchemaRegistryClient client) {
        inner = new KafkaAvroDeserializer(client);
    }

    public SpecificAvroDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
        inner = new KafkaAvroDeserializer(client, props);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> effectiveConfigs = new HashMap<>(configs);
        effectiveConfigs.put(SPECIFIC_AVRO_READER_CONFIG, true);
        inner.configure(effectiveConfigs, isKey);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(String s, byte[] bytes) {
        return (T) inner.deserialize(s, bytes);
    }

    @Override
    public void close() {
        inner.close();
    }
}