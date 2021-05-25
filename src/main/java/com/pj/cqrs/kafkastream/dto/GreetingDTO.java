package com.pj.cqrs.kafkastream.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class GreetingDTO implements Serializable
{
	private static final long serialVersionUID = 2729048783015827572L;

	private String message;
	private String timestamp;

	public GreetingDTO()
	{
	}

	public GreetingDTO(String message, String timestamp)
	{
		this.message = message;
		this.timestamp = timestamp;
	}
}
