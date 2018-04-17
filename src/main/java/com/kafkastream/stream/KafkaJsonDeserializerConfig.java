package com.kafkastream.stream;

import io.confluent.common.config.ConfigDef;

import java.util.Map;

/**
 * Deserializer configuration for {@link KafkaJsonDeserializer}. It's slightly odd
 * to have this extend {@link KafkaJsonDecoderConfig} (in most other cases the
 * Decoder variants extend from the Deserializer version), but the reason is that
 * {@link kafka.serializer.Decoder} doesn't provide the ability to distinguish
 * between constructing serializers for keys or for values. So the type configuration
 * options do not work with {@link KafkaJsonDecoder}.
 */
public class KafkaJsonDeserializerConfig extends KafkaJsonDecoderConfig {

    public static final String JSON_KEY_TYPE = "json.key.type";
    public static final String JSON_KEY_TYPE_DEFAULT = Object.class.getName();
    public static final String JSON_KEY_TYPE_DOC =
            "Classname of the type that the message key should be deserialized to";

    public static final String JSON_VALUE_TYPE = "json.value.type";
    public static final String JSON_VALUE_TYPE_DEFAULT = Object.class.getName();
    public static final String JSON_VALUE_TYPE_DOC =
            "Classname of the type that the message value should be deserialized to";

    private static ConfigDef config;

    static {
        config = baseConfig()
                .define(JSON_KEY_TYPE, ConfigDef.Type.CLASS, JSON_KEY_TYPE_DEFAULT,
                        ConfigDef.Importance.MEDIUM, JSON_KEY_TYPE_DOC)
                .define(JSON_VALUE_TYPE, ConfigDef.Type.CLASS, JSON_VALUE_TYPE_DEFAULT,
                        ConfigDef.Importance.MEDIUM, JSON_VALUE_TYPE_DOC);
    }

    public KafkaJsonDeserializerConfig(Map<?, ?> props) {
        super(config, props);
    }

}