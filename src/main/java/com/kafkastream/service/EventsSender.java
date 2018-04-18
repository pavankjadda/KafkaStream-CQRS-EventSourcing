package com.kafkastream.service;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Service;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


@Service
@EnableAutoConfiguration
public class EventsSender
{
    private Properties properties;

    public EventsSender() throws UnknownHostException
    {
        this.properties = new Properties();
        properties.put("application.id", "cqrs-streams");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.serializer", Serdes.String().serializer().getClass());
        //properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.serializer", SpecificAvroSerializer.class);
    }

    public void sendGreetingsEvent(Greetings greetings) throws ExecutionException, InterruptedException
    {
        Producer<String, Greetings> kafkaGreetingsProducer = new KafkaProducer<>(properties);
        ProducerRecord<String, Greetings> greetingsRecord = new ProducerRecord<>("greetings", greetings.getMessage(), greetings);
        Future<RecordMetadata> future = kafkaGreetingsProducer.send(greetingsRecord);
        System.out.println("Greetings record Sent. Greetings message: " + greetings.getMessage());
        System.out.println("Greetings future.get(): " + future.get());

    }


    public void sendCustomerEvent(Customer customer) throws ExecutionException, InterruptedException
    {
        Producer<String, Customer> kafkaProducerCustomer = new KafkaProducer<>(properties);
        ProducerRecord<String, Customer> customerRecord = new ProducerRecord<>("customer", customer.getCustomerId(), customer);
        Future<RecordMetadata> future = kafkaProducerCustomer.send(customerRecord);
        System.out.println("Customer record sent. Customer Id: " + customer.getCustomerId());
        System.out.println("Customer future.get(): " + future.get());
    }

    public void sendOrderEvent(Order order) throws ExecutionException, InterruptedException
    {
        Producer<String, Order> kafkaOrderProducer = new KafkaProducer<>(properties);
        ProducerRecord<String, Order> orderRecord = new ProducerRecord<>("order", order.getOrderId(), order);
        Future<RecordMetadata> future = kafkaOrderProducer.send(orderRecord);
        System.out.println("Customer order sent. Order Id: " + order.getOrderId());
        System.out.println("Order future.get(): " + future.get());
    }
    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {

        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }


}
