package com.kafkastream.service;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import com.sun.tools.corba.se.idl.constExpr.Or;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class EventsListener
{
    /*@StreamListener
    public void handleGreetings(@Input(GreetingsStreams.INPUT) KStream<String,GreetingsEvent> greetingsEventKStream)
    {
        greetingsEventKStream.foreach((key, value) -> System.out.println("Greetings Message: "+value.getMessage()));
    }*/

    public static void main(String[] args)
    {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"cqrs-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.deserializer", SpecificAvroDeserializer.class);
        properties.put("value.deserializer", SpecificAvroDeserializer.class);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<Order> orderSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<Greetings> greetingsSerde = createSerde("http://localhost:8081");

        KStream<String, Customer> customerKStream = streamsBuilder.stream("customer",Consumed.with(Serdes.String(), customerSerde));
        customerKStream.foreach(((key, value) -> System.out.println("Customer value from Topic:  " + value.toString())));

        KStream<String, Order> orderKStream = streamsBuilder.stream("order",Consumed.with(Serdes.String(), orderSerde));
        orderKStream.foreach(((key, value) -> System.out.println("Order value from Topic:  " + value.toString())));

        KStream<String, Greetings> greetingsKStream = streamsBuilder.stream("greetings",Consumed.with(Serdes.String(), greetingsSerde));
        greetingsKStream.foreach(((key, value) -> System.out.println("Greeting message from Topic:  " + value.toString())));

        KStream<String, Order> modifiedOrderKStream=orderKStream.selectKey((key, value) -> value.getCustomerId().toString());
        modifiedOrderKStream.foreach(((key, value) -> System.out.println("Modified Key message from Order:  " + key)));

        KStream<String, String> customersOrders = customerKStream.leftJoin(modifiedOrderKStream, new ValueJoiner<Customer, Order, String>()
        {
            @Override
            public String apply(Customer customer, Order order)
            {
                return customer+" created "+ order;
            }
        }, JoinWindows.of(TimeUnit.MINUTES.toMillis(5)), Joined.with(Serdes.String(), customerSerde, orderSerde));
        customersOrders.foreach(((key, value) -> System.out.println("customer orders:  " + value)));

        /*
        KTable<String,String> orderKTable=streamsBuilder.table("order");
        orderKTable.foreach(((key, value) -> System.out.println("Order from Topic: "+value)));

        KTable<String,String>   customerKTable = streamsBuilder.table("customer");
        customerKTable.foreach(((key, value) -> System.out.println("Retrieved Customer from Topic: " + value)));
        KTable<String,String>   customerOrdersKTable=orderKTable.leftJoin(customerKTable,(order,customer)-> order+" and "+customer);
        customerOrdersKTable.foreach(((key, value) -> System.out.println("customerOrders : "+value)));
        */

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run()
            {
                streams.close();
                latch.countDown();
            }
        });


        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.cleanUp();
            streams.start();
            latch.await();

        } catch (Throwable e)
        {
            System.exit(1);
        }

        System.exit(0);
    }
    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {

        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }

}
