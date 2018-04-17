package com.kafkastream.stream;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class KafkaJsonDecoder<T> extends KafkaJsonDeserializer<T> implements Decoder<T>
{
    /**
     * This constructor is needed by Kafka. When used directly or through reflection,
     * the type will default to {@link Object} since there is no way to infer a generic type.
     * In particular, it is an error to do the following:
     *
     * <pre>{@code
     *   // WARNING: DON'T DO THIS
     *   KafkaJsonDecoder<String> decoder = new KafkaJsonDecoder<String>(props);
     *   }
     * </pre>
     *
     * <p>Instead you must use {@link KafkaJsonDecoder#KafkaJsonDecoder(VerifiableProperties, Class)}
     * with the expected type.
     *
     * @param props The decoder configuration
     */

    @SuppressWarnings({"rawtypes", "unchecked"})
    public KafkaJsonDecoder(VerifiableProperties props) {
        configure(new KafkaJsonDecoderConfig(props.props()), (Class<T>) Object.class);
    }

    /**
     * Create a new decoder using the expected type to decode to
     *
     * @param props The decoder configuration
     * @param type  The type of objects constructed
     */
    public KafkaJsonDecoder(VerifiableProperties props, Class<T> type) {
        configure(new KafkaJsonDecoderConfig(props.props()), type);
    }

    @Override
    public T fromBytes(byte[] bytes) {
        return super.deserialize(null, bytes);
    }
}
