package com.kafkastream.constants;

public class KafkaConstants
{
    //Kafka Streams Configuration
    public static String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static String APPLICATION_ID_CONFIG = "cqrs-streams";
    public static String APPLICATION_SERVER_CONFIG = "localhost:8095";
    public static String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    public static String COMMIT_INTERVAL_MS_CONFIG = "2000";
    public static String AUTO_OFFSET_RESET_CONFIG = "earliest";


    //Kafka REST Proxy
    public static String REST_PROXY_HOST = "localhost";
    public static int REST_PROXY_PORT = 8095;


    //Kafka Topics
    public static String ORDER_TOPIC = "order";
    public static String CUSTOMER_TOPIC = "customer";
    public static String CUSTOMER_ORDER_TOPIC = "customer-order";
    public static String GREETINGS_TOPIC = "greetings";
    public static String ORDER_TO_KTABLE_TOPIC = "order-to-ktable";


    //Kafka State Stores
    public static String CUSTOMER_ORDER_STORE_NAME = "customer-order-store";
    public static String CUSTOMER_STORE_NAME = "customer-store";
    public static String ORDER_STORE_NAME = "order-store";
    public static String GREETING_STORE_NAME = "greetings-store";

}
