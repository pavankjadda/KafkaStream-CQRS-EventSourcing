package com.kafkastream.stream;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GenericSerializer<T>   implements Serializer<T>
{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {

    }

    @Override
    public byte[] serialize(String topic, T data)
    {
        return data.toString().getBytes();

    }

    @Override
    public void close()
    {

    }
}
