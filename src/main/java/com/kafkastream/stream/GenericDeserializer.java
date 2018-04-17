package com.kafkastream.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkastream.model.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;

public class GenericDeserializer<T> implements Deserializer<T>
{
    KafkaAvroDeserializer inner;
    public GenericDeserializer()
    {
        inner=new KafkaAvroDeserializer();
    }
    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        Map<String, Object> effectiveConfigs = new HashMap<>(configs);
        effectiveConfigs.put(SPECIFIC_AVRO_READER_CONFIG, true);
        inner.configure(effectiveConfigs, isKey);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(String topic, byte[] data)
    {
        ObjectMapper objectMapper=new ObjectMapper();

        return objectMapper.readValue(data,type);
    }


    @Override
    public void close()
    {

    }
}
