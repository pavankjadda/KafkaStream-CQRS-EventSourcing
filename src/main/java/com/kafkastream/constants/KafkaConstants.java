package com.kafkastream.constants;

public class KafkaConstants
{
    public static String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static String APPLICATION_ID_CONFIG = "cqrs-streams";
    public static String APPLICATION_SERVER_CONFIG = "localhost:8095";
    public static String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    public static String COMMIT_INTERVAL_MS_CONFIG = "2000";
    public static String AUTO_OFFSET_RESET_CONFIG = "earliest";


    public static String REST_PROXY_HOST = "localhost";
    public static int REST_PROXY_PORT = 8095;


    //Kafka Topics
    public static String ORDER_TOPIC = "order";
    public static String CUSTOMER_TOPIC = "customer";
    public static String CUSTOMER_ORDER_TOPIC = "customer-order";
    public static String GREETINGS_TOPIC = "greetings";
    public static String ORDER_TO_KTABLE_TOPIC = "order-to-ktable";


    //Kafka State Stores
    public static String CUSTOMER_ORDERS_STORE_NAME = "customerordersstore";

}
