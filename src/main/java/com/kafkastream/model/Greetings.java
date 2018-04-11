package com.kafkastream.model;


public class Greetings
{
    private String timestamp;
    private String message;

    public Greetings()
    {
    }

    public Greetings(String message,String timestamp)
    {
        this.timestamp = timestamp;
        this.message = message;
    }

    public String getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(String timestamp)
    {
        this.timestamp = timestamp;
    }

    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }

    @Override
    public String toString()
    {
        return "Greetings{" + "timestamp=" + timestamp + ", message='" + message + '\'' + '}';
    }
}
