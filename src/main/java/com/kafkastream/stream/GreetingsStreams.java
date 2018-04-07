package com.kafkastream.stream;

import com.kafkastream.event.GreetingsEvent;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface GreetingsStreams
{
    String INPUT = "greetings-in";
    String OUTPUT = "greetings-out";

    @Input(INPUT)
    KStream<String,GreetingsEvent> incomingGreetings();

    @Output(OUTPUT)
    MessageChannel outgoingGreetings();

}
