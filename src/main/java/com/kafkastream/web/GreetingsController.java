package com.kafkastream.web;

import com.kafkastream.model.Greetings;
import com.kafkastream.service.GreetingsSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
public class GreetingsController
{
    @Autowired
    private GreetingsSender greetingsSender;

    @GetMapping("/greetings")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Greetings send()
    {
        Random  random=new Random();
        String message = "Message "+random.nextInt();
        Greetings greetings = new Greetings(message,System.currentTimeMillis());
        greetingsSender.send(greetings);
        return greetings;
    }
}
