package com.kafkastream.config;


import com.kafkastream.stream.GenericStreams;
import org.springframework.cloud.stream.annotation.EnableBinding;

@EnableBinding(GenericStreams.class)
public class AppConfig
{

}
