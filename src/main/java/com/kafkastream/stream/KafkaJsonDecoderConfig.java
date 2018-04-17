package com.kafkastream.stream;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;

import java.util.Map;

public class KafkaJsonDecoderConfig extends AbstractConfig
{
    public static final String FAIL_UNKNOWN_PROPERTIES = "json.fail.unknown.properties";
    public static final boolean FAIL_UNKNOWN_PROPERTIES_DEFAULT = true;
    public static final String FAIL_UNKNOWN_PROPERTIES_DOC = "Whether to fail deserialization if unknown JSON properties are encountered";

    public KafkaJsonDecoderConfig(Map<?, ?> props)
    {
        super(baseConfig(), props);
    }

    protected KafkaJsonDecoderConfig(ConfigDef config, Map<?, ?> props)
    {
        super(config, props);
    }

    protected static ConfigDef baseConfig()
    {
        return new ConfigDef().define(FAIL_UNKNOWN_PROPERTIES, ConfigDef.Type.BOOLEAN, FAIL_UNKNOWN_PROPERTIES_DEFAULT, ConfigDef.Importance.LOW, FAIL_UNKNOWN_PROPERTIES_DOC);
    }
}
