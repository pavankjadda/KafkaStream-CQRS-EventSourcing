package com.kafkastream.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class GreetingDto implements Serializable
{
    private static final long serialVersionUID = 2729048783015827572L;

    private String message;

    private String timestamp;

    public GreetingDto()
    {
    }

    public GreetingDto(String message, String timestamp)
    {
        this.message = message;
        this.timestamp = timestamp;
    }
}
