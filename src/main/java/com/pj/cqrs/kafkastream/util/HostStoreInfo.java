package com.pj.cqrs.kafkastream.util;

import lombok.Data;

import java.util.Set;

/**
 * A simple bean that can be JSON serialized via Jersey. Represents a KafkaStreams instance
 * that has a set of state stores. See for how it is used.
 * <p>
 * We use this JavaBean based approach as it fits nicely with JSON serialization provided by
 * jax-rs/jersey
 */
@Data
public class HostStoreInfo
{

	private String host;
	private int port;
	private Set<String> storeNames;

	public HostStoreInfo(final String host, final int port, final Set<String> storeNames)
	{
		this.host = host;
		this.port = port;
		this.storeNames = storeNames;
	}
}
