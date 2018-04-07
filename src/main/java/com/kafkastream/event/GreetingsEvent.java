package com.kafkastream.event;

import com.kafkastream.model.Greetings;

public class GreetingsEvent
{
    private Greetings   greetings;
    private String  message;

    public GreetingsEvent()
    {

    }


    public GreetingsEvent(Greetings greetings, String message)
    {
        this.greetings = greetings;
        this.message = message;
    }

    public Greetings getGreetings()
    {
        return greetings;
    }

    public void setGreetings(Greetings greetings)
    {
        this.greetings = greetings;
    }

    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }
}
