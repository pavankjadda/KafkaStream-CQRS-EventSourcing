package com.kafkastream.service;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class EventsListener
{
    /*@StreamListener
    public void handleGreetings(@Input(GreetingsStreams.INPUT) KStream<String,GreetingsEvent> greetingsEventKStream)
    {
        greetingsEventKStream.foreach((key, value) -> System.out.println("Greetings Message: "+value.getMessage()));
    }*/

    public static void main(String[] args)
    {
        //SpringApplication.run(KafkaStreamApplication.class, args);
        //Copied from GreetingsEventSink method
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-greetings");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder streamsBuilder=new StreamsBuilder();


        KStream<String, String> customerKStream =streamsBuilder.stream("customer");
        customerKStream.foreach(((key, value) -> System.out.println("Customer from Topic: "+value.toLowerCase())));

        KStream<String, String> orderKStream =streamsBuilder.stream("order");
        orderKStream.foreach(((key, value) -> System.out.println("Order from Topic: "+value.toLowerCase())));


//        KTable<String,Customer> customerKTable=streamsBuilder.table("customer");
//        customerKTable.foreach(((key, value) -> System.out.println("Customer from KTable: "+value)));
//        KTable<String,Order>   orderKTable=streamsBuilder.table("order");
//        orderKTable.foreach(((key, value) -> System.out.println("Order from KTable: "+value)));
//
//        KTable<String,Object>   customerOrdersKTable=orderKTable.leftJoin(customerKTable,(order,customer)-> order.getCustomerId()==customer.getCustomerId());
//        customerOrdersKTable.foreach(((key, value) -> System.out.println("Join Table Value: "+value)));


        Topology topology=streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        }

        catch (Throwable e)
        {
            System.exit(1);
        }
        System.exit(0);
    }


}
