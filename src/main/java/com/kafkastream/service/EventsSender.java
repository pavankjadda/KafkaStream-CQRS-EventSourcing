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
import org.apache.kafka.streams.StreamsBuilder;
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

    private StreamsBuilder streamsBuilder;


    public EventsSender()
    {
        this.properties = new Properties();
        properties.put("application.id", "cqrs-streams");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("group.id", "cqrs");
        properties.put("commit.interval.ms","100");
        properties.put("topic.metadata.refresh.interval.ms","100");
        properties.put("acks", "all");
        properties.put("key.serializer", Serdes.String().serializer().getClass());
        properties.put("value.serializer", SpecificAvroSerializer.class);

        this.streamsBuilder=new StreamsBuilder();
    }

    public void sendGreetingsEvent(Greetings greetings) throws ExecutionException, InterruptedException
    {
        Producer<String, Greetings> kafkaGreetingsProducer = new KafkaProducer<>(properties);
        ProducerRecord<String, Greetings> greetingsRecord = new ProducerRecord<>("greetings", greetings.getMessage().toString(), greetings);
        Future<RecordMetadata> future = kafkaGreetingsProducer.send(greetingsRecord);
        System.out.println("Greetings record Sent. Greetings message: " + greetings.getMessage());
        System.out.println("Greetings future.get(): " + future.get());

    }


    public void sendCustomerEvent(Customer customer) throws ExecutionException, InterruptedException
    {
        SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");
        Producer<String, Customer> kafkaProducerCustomer = new KafkaProducer<>(properties,Serdes.String().serializer(),customerSerde.serializer());
        ProducerRecord<String, Customer> customerRecord = new ProducerRecord<>("customer", customer.getCustomerId().toString(), customer);
        Future<RecordMetadata> future = kafkaProducerCustomer.send(customerRecord);
        System.out.println("Customer record sent. Customer Id: " + customer.getCustomerId());
        System.out.println("Customer future.get(): " + future.get());
    }

    public void sendOrderEvent(Order order) throws ExecutionException, InterruptedException
    {
        SpecificAvroSerde<Order> orderSerde = createSerde("http://localhost:8081");
        Producer<String, Order> kafkaOrderProducer = new KafkaProducer<>(properties,Serdes.String().serializer(),orderSerde.serializer());
        ProducerRecord<String, Order> orderRecord = new ProducerRecord<>("order", order.getOrderId().toString(), order);
        Future<RecordMetadata> future = kafkaOrderProducer.send(orderRecord);
        System.out.println("Order sent. Order Id: " + order.getOrderId());
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
