package com.kafkastream.stream;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * Encode an Object to a JSON-encoded byte array.
 */
public class KafkaJsonEncoder<T> extends KafkaJsonSerializer<T> implements Encoder<T> {

    public KafkaJsonEncoder(VerifiableProperties props) {
        configure(new KafkaJsonSerializerConfig(props.props()));
    }

    @Override
    public byte[] toBytes(T val) {
        return serialize(null, val);
    }
}