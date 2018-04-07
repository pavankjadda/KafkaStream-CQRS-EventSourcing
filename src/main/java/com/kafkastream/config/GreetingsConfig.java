package com.kafkastream.config;


import com.kafkastream.stream.GreetingsStreams;
import org.springframework.cloud.stream.annotation.EnableBinding;

@EnableBinding(GreetingsStreams.class)
public class GreetingsConfig
{

}
