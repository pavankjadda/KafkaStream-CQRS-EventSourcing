package com.kafkastream.events.services;

import com.kafkastream.constants.KafkaConstants;
import com.kafkastream.model.Customer;
import com.kafkastream.model.CustomerOrder;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Service;

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

    public EventsSender()
    {
        this.properties = new Properties();
        properties.put("bootstrap.servers", KafkaConstants.BOOTSTRAP_SERVERS_CONFIG);
        properties.put("acks", "all");
        properties.put("key.serializer", Serdes.String().serializer().getClass());
        properties.put("value.serializer", SpecificAvroSerializer.class);
    }

    public void sendGreetingsEvent(Greetings greetings) throws ExecutionException, InterruptedException
    {
        SpecificAvroSerde<Greetings> greetingsSerde = createSerde(KafkaConstants.SCHEMA_REGISTRY_URL);
        Producer<String, Greetings> kafkaGreetingsProducer = new KafkaProducer<>(properties, Serdes.String().serializer(),greetingsSerde.serializer());
        ProducerRecord<String, Greetings> greetingsRecord = new ProducerRecord<>("greetings", greetings.getMessage().toString(), greetings);
        Future<RecordMetadata> future = kafkaGreetingsProducer.send(greetingsRecord);
        System.out.println("Greetings record Sent. Greetings message: " + greetings.getMessage());
        System.out.println("Greetings future.get(): " + future.get());

    }


    public void sendCustomerEvent(Customer customer) throws ExecutionException, InterruptedException
    {
        SpecificAvroSerde<Customer> customerSerde = createSerde(KafkaConstants.SCHEMA_REGISTRY_URL);
        Producer<String, Customer> kafkaProducerCustomer = new KafkaProducer<>(properties,Serdes.String().serializer(),customerSerde.serializer());
        ProducerRecord<String, Customer> customerRecord = new ProducerRecord<>("customer", customer.getCustomerId().toString(), customer);
        Future<RecordMetadata> future = kafkaProducerCustomer.send(customerRecord);
        System.out.println("Customer record sent. Customer Id: " + customer.getCustomerId());
        System.out.println("Customer future.get(): " + future.get());
    }

    public void sendOrderEvent(Order order) throws ExecutionException, InterruptedException
    {
        SpecificAvroSerde<Order> orderSerde = createSerde(KafkaConstants.SCHEMA_REGISTRY_URL);
        Producer<String, Order> kafkaOrderProducer = new KafkaProducer<>(properties,Serdes.String().serializer(),orderSerde.serializer());
        ProducerRecord<String, Order> orderRecord = new ProducerRecord<>("order", order.getOrderId().toString(), order);
        Future<RecordMetadata> future = kafkaOrderProducer.send(orderRecord);
        System.out.println("Order sent. Order Id: " + order.getOrderId());
        System.out.println("Order future.get(): " + future.get());
    }


    public void sendCustomerOrderEvent(CustomerOrder customerOrder) throws ExecutionException, InterruptedException
    {
        SpecificAvroSerde<CustomerOrder> customerOrderSerde = createSerde(KafkaConstants.SCHEMA_REGISTRY_URL);
        Producer<String, CustomerOrder> kafkaCustomerOrderProducer = new KafkaProducer<>(properties, Serdes.String().serializer(),customerOrderSerde.serializer());
        ProducerRecord<String, CustomerOrder> customerOrderRecord = new ProducerRecord<>("customer-order", customerOrder.getOrderId().toString(), customerOrder);
        Future<RecordMetadata> future = kafkaCustomerOrderProducer.send(customerOrderRecord);

    }
    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {
        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }


}
