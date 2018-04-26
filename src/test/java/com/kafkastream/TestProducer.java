package com.kafkastream;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Order;
import com.kafkastream.service.EventsSender;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestProducer
{
    @Autowired
    private EventsSender eventsSender;

    private Properties properties;

    private StreamsBuilder streamsBuilder;

    @Before
    public void setUp()
    {
        //When configuring the default serdes of StreamConfig
        properties = new Properties();
        properties.put("application.id", "cqrs-streams");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("commit.interval.ms","100");
        properties.put("acks", "all");
        properties.put("key.serializer", Serdes.String().serializer().getClass());
        properties.put("value.serializer", SpecificAvroSerializer.class);
        streamsBuilder = new StreamsBuilder();
    }

    @Test
    public void sendCustomer() throws ExecutionException, InterruptedException
    {
        // When you want to override serdes explicitly/selectively
        SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");
        Producer<String, Customer> producer = new KafkaProducer<>(properties, Serdes.String().serializer(), customerSerde.serializer());

        Customer customer = new Customer();
        customer.setCustomerId("CU1001");
        customer.setFirstName("John");
        customer.setLastName("Doe");
        customer.setEmail("john.doe@mail.com");
        customer.setPhone("993-332-9832");

        ProducerRecord<String, Customer> customerRecord = new ProducerRecord<>("customer", customer.getCustomerId().toString(), customer);
        Future<RecordMetadata> future = producer.send(customerRecord);
        System.out.println("Customer record sent. Customer Id: " + customer.getCustomerId());
        System.out.println("Customer future.get(): " + future.get());

    }

    @Test
    public void sendOrder() throws ExecutionException, InterruptedException
    {
        Random random = new Random(1);

        //Send Order Event
        Order order = new Order();
        order.setOrderId("ORD" + random.nextInt(10000));
        order.setCustomerId("CU8985");
        order.setOrderItemName("Reebok Shoes");
        order.setOrderPlace("NewYork,NY");
        order.setOrderPurchaseTime(getCurrentTime());

        // When you want to override serdes explicitly/selectively
        SpecificAvroSerde<Order> orderSerde = createSerde("http://localhost:8081");
        Producer<String, Order> kafkaOrderProducer = new KafkaProducer<>(properties, Serdes.String().serializer(), orderSerde.serializer());

        ProducerRecord<String, Order> orderRecord = new ProducerRecord<>("order", order.getOrderId().toString(), order);
        Future<RecordMetadata> future = kafkaOrderProducer.send(orderRecord);
        System.out.println("Order sent. Order Id: " + order.getOrderId());
        System.out.println("Order future.get(): " + future.get());
    }

    @Test
    public void sendOrderToKTable() throws ExecutionException, InterruptedException
    {
        Random random = new Random(1);

        //Send Order Event
        Order order = new Order();
        order.setOrderId("ORD" + random.nextInt(10000));
        order.setCustomerId("CU1001");
        order.setOrderItemName("Reebok Shoes");
        order.setOrderPlace("NewYork,NY");
        order.setOrderPurchaseTime(getCurrentTime());

        // When you want to override serdes explicitly/selectively
        SpecificAvroSerde<Order> orderSerde = createSerde("http://localhost:8081");
        Producer<String, Order> kafkaOrderProducer = new KafkaProducer<>(properties, Serdes.String().serializer(), orderSerde.serializer());

        ProducerRecord<String, Order> orderRecord = new ProducerRecord<>("order-to-ktable-topic", order.getCustomerId().toString(), order);
        Future<RecordMetadata> future = kafkaOrderProducer.send(orderRecord);
        System.out.println("Order sent. Order Id: " + order.getOrderId());
        System.out.println("Order future.get(): " + future.get());
    }


    private <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {
        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }

    private String getCurrentTime()
    {
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
        return calendar.getTime().toString();
    }

}
