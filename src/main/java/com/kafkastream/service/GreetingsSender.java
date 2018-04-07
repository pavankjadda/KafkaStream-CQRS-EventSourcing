package com.kafkastream.service;

import com.kafkastream.event.GreetingsEvent;
import com.kafkastream.model.Greetings;
import com.kafkastream.stream.GreetingsStreams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;


@Service
@Slf4j
@EnableAutoConfiguration
public class GreetingsSender
{
    @Autowired
    private GreetingsStreams greetingsStreams;


    public void send(Greetings greetings)
    {
        GreetingsEvent greetingsEvent = new GreetingsEvent(greetings, greetings.getMessage());
        Message<GreetingsEvent> message = MessageBuilder.withPayload(greetingsEvent).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
        greetingsStreams.outgoingGreetings().send(message);
    }

}
