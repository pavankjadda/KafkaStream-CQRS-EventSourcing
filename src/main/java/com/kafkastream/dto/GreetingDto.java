package com.kafkastream.dto;

import lombok.Data;

@Data
public class GreetingDto
{
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
