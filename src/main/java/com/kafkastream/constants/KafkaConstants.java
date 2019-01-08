package com.kafkastream.constants;

public class KafkaConstants
{
    public static String schemaRegistryUrl = "http://localhost:8081";
    public static String APPLICATION_ID_CONFIG = "cqrs-streams";
    public static String APPLICATION_SERVER_CONFIG = "localhost:8095";
    public static String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    public static String COMMIT_INTERVAL_MS_CONFIG = "2000";
    public static String AUTO_OFFSET_RESET_CONFIG = "earliest";


    public static String REST_PROXY_HOST = "localhost";
    public static int REST_PROXY_PORT = 8095;


    public static String orderTopic = "order";
    public static String customerTopic = "customer";
    public static String customerOrderTopic = "customer-order";
    public static String greetingsTopic = "greetings";
    public static String orderToKtableTopic = "order-to-ktable";

}
