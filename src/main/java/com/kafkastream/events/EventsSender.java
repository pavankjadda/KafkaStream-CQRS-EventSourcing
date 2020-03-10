package com.kafkastream.events;

import com.kafkastream.constants.KafkaConstants;
import com.kafkastream.model.Customer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


@Service
public class EventsSender
{
    private Properties properties;
    private static final Logger logger= LoggerFactory.getLogger(EventsSender.class);

    public EventsSender()
    {
        this.properties = new Properties();
        properties.put("bootstrap.servers", KafkaConstants.BOOTSTRAP_SERVERS_CONFIG);
        properties.put("acks", "all");
        properties.put("key.serializer", Serdes.String().serializer().getClass());
        properties.put("value.serializer", SpecificAvroSerializer.class);
    }

    // Send Customer Event
    public void sendCustomerEvent(Customer customer) throws ExecutionException, InterruptedException
    {
        SpecificAvroSerde<Customer> customerSerde = createSerde();
        Producer<String, Customer> kafkaProducerCustomer = new KafkaProducer<>(properties,Serdes.String().serializer(),customerSerde.serializer());
        ProducerRecord<String, Customer> customerRecord = new ProducerRecord<>("customer", customer.getCustomerId().toString(), customer);
        Future<RecordMetadata> future = kafkaProducerCustomer.send(customerRecord);
        logger.info("Customer record sent. Customer Id: {}", customer.getCustomerId());
        logger.info("Customer future.get(): {}" , future.get());
    }

    // Send Order Event
    public void sendOrderEvent(Order order) throws ExecutionException, InterruptedException
    {
        SpecificAvroSerde<Order> orderSerde = createSerde();
        Producer<String, Order> kafkaOrderProducer = new KafkaProducer<>(properties,Serdes.String().serializer(),orderSerde.serializer());
        ProducerRecord<String, Order> orderRecord = new ProducerRecord<>("order", order.getOrderId().toString(), order);
        Future<RecordMetadata> future = kafkaOrderProducer.send(orderRecord);
        logger.info("Order sent. Order Id: {}" ,order.getOrderId());
        logger.info("Order future.get(): {}" , future.get());
    }

    // Send Greetings Event
    public void sendGreetingsEvent(Greetings greetings) throws ExecutionException, InterruptedException
    {
        SpecificAvroSerde<Greetings> greetingsSerde = createSerde();
        Producer<String, Greetings> kafkaGreetingsProducer = new KafkaProducer<>(properties, Serdes.String().serializer(),greetingsSerde.serializer());
        ProducerRecord<String, Greetings> greetingsRecord = new ProducerRecord<>("greetings", greetings.getMessage().toString(), greetings);
        Future<RecordMetadata> future = kafkaGreetingsProducer.send(greetingsRecord);
        logger.info("Greetings record Sent. Greetings message: {}", greetings.getMessage());
        logger.info("Greetings future.get(): {}" , future.get());
    }

    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde()
    {
        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConstants.SCHEMA_REGISTRY_URL);
        serde.configure(serdeConfig, false);
        return serde;
    }
}
