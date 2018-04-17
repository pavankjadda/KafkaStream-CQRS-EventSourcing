package com.kafkastream.stream;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;

import java.util.Map;

public class KafkaJsonSerializerConfig extends AbstractConfig
{

    public static final String JSON_INDENT_OUTPUT = "json.indent.output";
    public static final boolean JSON_INDENT_OUTPUT_DEFAULT = false;
    public static final String JSON_INDENT_OUTPUT_DOC = "Whether JSON output should be indented (\"pretty-printed\")";

    private static ConfigDef config;

    static
    {
        config = new ConfigDef().define(JSON_INDENT_OUTPUT, ConfigDef.Type.BOOLEAN, JSON_INDENT_OUTPUT_DEFAULT, ConfigDef.Importance.LOW, JSON_INDENT_OUTPUT_DOC);
    }

    public KafkaJsonSerializerConfig(Map<?, ?> props)
    {
        super(config, props);
    }

}