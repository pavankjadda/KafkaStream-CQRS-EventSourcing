package com.kafkastream.stream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaJsonDeserializer<T> implements Deserializer<T>
{
    private ObjectMapper objectMapper;
    private Class<T> type;

    /**
     * Default constructor needed by Kafka
     */
    public KafkaJsonDeserializer()
    {

    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey)
    {
        configure(new KafkaJsonDeserializerConfig(props), isKey);
    }

    protected void configure(KafkaJsonDecoderConfig config, Class<T> type)
    {
        this.objectMapper = new ObjectMapper();
        this.type = type;

        boolean failUnknownProperties = config.getBoolean(KafkaJsonDeserializerConfig.FAIL_UNKNOWN_PROPERTIES);
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, failUnknownProperties);
    }

    @SuppressWarnings("unchecked")
    private void configure(KafkaJsonDeserializerConfig config, boolean isKey)
    {
        if (isKey)
        {
            configure(config, (Class<T>) config.getClass(KafkaJsonDeserializerConfig.JSON_KEY_TYPE));
        } else
        {
            configure(config, (Class<T>) config.getClass(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE));
        }

    }

    @Override
    public T deserialize(String ignored, byte[] bytes)
    {
        if (bytes == null || bytes.length == 0)
        {
            return null;
        }

        try
        {
            return objectMapper.readValue(bytes, type);
        } catch (Exception e)
        {
            throw new SerializationException(e);
        }
    }

    protected Class<T> getType()
    {
        return type;
    }

    @Override
    public void close()
    {

    }
}
