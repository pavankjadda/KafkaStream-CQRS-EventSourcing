package com.kafkastream.service;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Order;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;

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
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-greetings");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

/*
        KStream<String, String> customerKStream = streamsBuilder.stream("customer");
        customerKStream.foreach(((key, value) -> System.out.println("Customer from Topic: " + value)));

        KStream<String, String> orderKStream = streamsBuilder.stream("order");
        orderKStream.foreach(((key, value) -> System.out.println("Order from Topic: " + value)));
        long joinWindowSizeMs = 5L * 60L * 1000L; // 5 minutes
        long windowRetentionTimeMs = 30L * 24L * 60L * 60L * 1000L; // 30 days


        // Java 8
        KStream<String, String> customersOrders = customerKStream.leftJoin(orderKStream,(customer,order)->customer+" "+order,
                JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
                Joined.with(
                        Serdes.String(),
                        Serdes.String(),
                        Serdes.String())
        );*/


        // Java 7 example
/*        KStream<String, String> customersOrders = customerKStream.leftJoin(orderKStream,
                new ValueJoiner<String, String, String>() {
                    @Override
                    public String apply(String leftValue, String rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                },
                JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
                Joined.with(
                        Serdes.String(), *//* key *//*
                        Serdes.String(),   *//* left value *//*
                        Serdes.String())  *//* right value *//*
        );*/


        //customersOrders.foreach(((key, value) -> System.out.println("Order from customersOrders: " + value)));


     /*   KTable<String,String>   customerKTable=streamsBuilder.table("customer",Consumed.with(Topology.AutoOffsetReset.EARLIEST),Materialized.as("customer-store"));
        //System.out.println("Store Name: " + customerKTable.queryableStoreName());
        customerKTable.foreach(((key, value) -> System.out.println("Customer from Topic: " + value)));*/

        KTable<String,String> orderKTable=streamsBuilder.table("order");
        orderKTable.foreach(((key, value) -> System.out.println("Order from Topic: "+value)));

        KTable<String,String>   customerKTable = streamsBuilder.table("customer",Materialized.as("customer-store"));
        customerKTable.foreach(((key, value) -> System.out.println("Retrieved Customer from Topic: " + value)));


        /*KTable<String,String>   customerOrdersKTable=orderKTable.leftJoin(customerKTable,(order,customer)-> order+" and "+customer);
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
