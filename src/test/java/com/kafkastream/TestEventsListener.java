package com.kafkastream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TestEventsListener
{

    public static void main(String[] args)
    {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-greetings");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();


        {
        /*
        KStream<String, String> customerKStream = streamsBuilder.stream("customer", Consumed.with(Serdes.String(), Serdes.String()).withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        customerKStream.foreach(((key, value) -> System.out.println("Customer key from Topic:  " + key)));

        KStream<String, String> orderKStream = streamsBuilder.stream("order");
        orderKStream.foreach(((key, value) -> System.out.println("Order key from Topic: " + key)));

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
        );


        //customersOrders.foreach(((key, value) -> System.out.println("Order from customersOrders: " + value)));

        KTable<String,String>   customerKTable=streamsBuilder.table("customer",Consumed.with(Topology.AutoOffsetReset.EARLIEST),Materialized.as("customer-store"));
        //System.out.println("Store Name: " + customerKTable.queryableStoreName());
        customerKTable.foreach(((key, value) -> System.out.println("Customer from Topic: " + value)));

        */
    }

        KTable<String,String> orderKTable=streamsBuilder.table("order",Consumed.with(Serdes.String(),Serdes.String()),
                                                                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("OrderKeyValueStore"));
        orderKTable.foreach(((key, value) -> System.out.println("Order from Topic: key-> "+key+" , value-> "+ value)));


        KTable<String,String>   customerKTable = streamsBuilder.table("customer",Consumed.with(Serdes.String(),Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("CustomerKeyValueStore"));
        customerKTable.foreach(((key, value) -> System.out.println("Retrieved Customer from Topic: key-> "+key+" , value-> "+ value)));

        KTable<String,String>   customerOrdersKTable=orderKTable.leftJoin(customerKTable,(order,customer)-> order+" and "+customer);
        customerOrdersKTable.foreach(((key, value) -> System.out.println("customerOrders from Topic: key-> "+key+" , value-> "+ value)));

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        streams.start();


        StoreBuilder orderKeyValueStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("OrderKeyValueStore"),Serdes.String(), Serdes.String())
                                           .withLoggingEnabled(new HashMap<>());
        streamsBuilder.addStateStore(orderKeyValueStore);
        // Get the key-value store OrderKeyValueStore
        ReadOnlyKeyValueStore<String, String> keyValueStore =streams.store("OrderKeyValueStore", QueryableStoreTypes.keyValueStore());
        // Get value by key
        System.out.println("keyValueStore.approximateNumEntries(): "+keyValueStore.approximateNumEntries());
        System.out.println("Order Value:" + keyValueStore.get("ORD1001"));

      /*  Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
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

        } catch (Throwable e)
        {
            System.exit(1);
        }

        System.exit(0);*/
    }

}
