package com.kafkastream.service;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import com.kafkastream.stream.GenericStreams;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.Future;


@Service
@Slf4j
@EnableAutoConfiguration
public class EventsSender
{
    private Producer<String, String> kafkaProducer;

    private Properties properties;


    public EventsSender() throws UnknownHostException
    {
        this.properties = new Properties();
        properties.put("application.id", InetAddress.getLocalHost().getHostName());
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    public void sendGreetingsEvent(Greetings greetings)
    {
        ProducerRecord<String, String> greetingsRecord = new ProducerRecord<>("greetings", greetings.getMessage(), greetings.toString());
        Future<RecordMetadata> future = kafkaProducer.send(greetingsRecord);
/*
        GreetingsEvent greetingsEvent = new GreetingsEvent(greetings, greetings.getMessage());
        Message<Greetings> message = MessageBuilder.withPayload(greetings).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
        genericStreams.outgoingGreetings().send(message);*/
    }


    public void sendCustomerEvent(Customer customer)
    {
        ProducerRecord<String, String> customerRecord = new ProducerRecord<>("customer", customer.getCustomerId(), customer.toString());
        Future<RecordMetadata> future = kafkaProducer.send(customerRecord);
/*
        CustomerEvent customerEvent = new CustomerEvent(customer, customer.getCustomerId());
        Message<Customer> message = MessageBuilder.withPayload(customer).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
        genericStreams.outgoingCustomers().send(message);*/
    }

    public void sendOrderEvent(Order order)
    {
        ProducerRecord<String, String> orderRecord = new ProducerRecord<>("order", order.getOrderId(), order.toString());
        Future<RecordMetadata> future = kafkaProducer.send(orderRecord);

/*        OrderEvent orderEvent = new OrderEvent(order, order.getOrderId());
        Message<Order> message = MessageBuilder.withPayload(order).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
        genericStreams.outgoingOrders().send(message);*/
    }


}
