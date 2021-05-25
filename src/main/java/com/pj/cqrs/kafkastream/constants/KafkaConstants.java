package com.pj.cqrs.kafkastream.constants;

public class KafkaConstants
{
	//Kafka Streams Configuration
	public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
	public static final String APPLICATION_ID_CONFIG = "cqrs-streams";
	public static final String APPLICATION_SERVER_CONFIG = "localhost:8095";
	public static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
	public static final String COMMIT_INTERVAL_MS_CONFIG = "2000";
	public static final String AUTO_OFFSET_RESET_CONFIG = "earliest";

	//Kafka REST Proxy
	public static final String REST_PROXY_HOST = "http://localhost";
	public static final int REST_PROXY_PORT = 8095;


	//Kafka Topics
	public static final String ORDER_TOPIC = "order";
	public static final String CUSTOMER_TOPIC = "customer";
	public static final String CUSTOMER_ORDER_TOPIC = "customer-order";
	public static final String GREETINGS_TOPIC = "greetings";
	public static final String ORDER_TO_KTABLE_TOPIC = "order-to-ktable";


	//Kafka State Stores
	public static final String CUSTOMER_ORDER_STORE_NAME = "customer-order-store";
	public static final String CUSTOMER_STORE_NAME = "customer-store";
	public static final String ORDER_STORE_NAME = "order-store";
	public static final String GREETING_STORE_NAME = "greetings-store";

	private KafkaConstants()
	{
		//Hides public constructor
	}
}
